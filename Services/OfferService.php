<?php

namespace App\Services;

use App\Enums\FulfillmentChannelEnum;
use App\Http\Resources\BuyboxActivityResource;
use App\Http\Resources\OfferResource;
use App\Http\Resources\OfferSummaryResource;
use App\Http\Resources\SalesRankResource;
use App\Jobs\MarkBuyboxActivityViewedJob;
use App\Models\BuyboxActivity;
use App\Models\Listing;
use App\Models\Notification;
use App\Models\Offer;
use App\Models\OfferSummary;
use App\Models\Seller;
use DateTime;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Arr;

class OfferService
{
    public function __construct(public Seller $seller) {}

    /**
     * Update the offers for a listing based on an ANY_OFFER_CHANGED notification.
     * 
     * @param array $data
     * @param Notification $notification
     * @return void
     */
    public function updateOffers(array $data, Notification $notification): void
    {
        $asin = $data['OfferChangeTrigger']['ASIN'];

        // get the listing that corresponds to this notification
        $listing = Listing::where('seller_id', $this->seller->id)->where('asin', $asin)->first();
        
        // if there is no listing recorded, create a new one
        if (is_null($listing)) {
            $service = new ListingService($this->seller);

            //rate limit for getPricing within getSingleListing is .5 requests /second
            logger()->info("Sleeping for 2 seconds before calling getSingleListing() method.");
            sleep(2);
            $listing = $service->getSingleListing($asin);
            if (is_null($listing)) {
                logger()->error("Could not retrieve listing data for asin `{$asin}` using getPricing(). Aborting processing of ANY_OFFER_CHANGED notification.");
                return;
            }
        }

        // Retrieve and filter offerSummary data
        $channels = [FulfillmentChannelEnum::FBA->value, FulfillmentChannelEnum::MERCHANT->value];
        $summary = array_fill_keys($channels, null);

        // iterate through all of the summary data in the notification
        foreach($data['Summary'] as $key => $arr) {
            foreach ($channels as $channel) {
                //Competitive price threshold doesn't depend on channel or condition, so always add it to the summary
                if ($key === 'CompetitivePriceThreshold') {
                    $summary[$channel][$key] = $arr;
                    continue;
                }

                $amazonChannelName = match ($channel) {
                    FulfillmentChannelEnum::FBA->value => 'Amazon',
                    FulfillmentChannelEnum::MERCHANT->value => 'Merchant',
                };
                //filter summary data to only be for new condition listings and for the relevant channel type
                $filtered = array_filter(
                    $arr,
                    fn($object) => 
                        !is_null(Arr::get($object, 'Condition'))
                        && strtolower($object['Condition']) === 'new'
                        && (isset($object['FulfillmentChannel']) ? $object['FulfillmentChannel'] === $amazonChannelName : true)
                );
                if (count($filtered) !== 0) {
                    $summary[$channel][$key] = array_values($filtered)[0];
                }
            }
        } 
        
        // get the offerSummaries that correspond to this listing
        $offerSummary = OfferSummary::byFulfillmentChannel($listing);

        // Update offerSummaries with the summary data from the notification
        foreach ($channels as $channel) {
            if (!is_null($summary[$channel])) {
                $toUpdate = array_filter([
                        'num_offers' => Arr::get($summary[$channel], 'NumberOfOffers.OfferCount'),
                        'lowest_price' => Arr::get($summary[$channel], 'LowestPrices.LandedPrice.Amount'),
                        'buybox_price' => Arr::get($summary[$channel], 'BuyBoxPrices.LandedPrice.Amount'),
                        'num_buybox_eligible_offers' => Arr::get($summary[$channel], 'NumberOfBuyBoxEligibleOffers.OfferCount'),
                        'competitive_price_threshold' => Arr::get($summary[$channel], 'CompetitivePriceThreshold.Amount'),
                        'event_time' => $notification->event_time,
                    ],
                    fn($val) => !is_null($val)
                );
                if (!isset($offerSummary[$channel])) {
                    $offerSummary[$channel] = OfferSummary::create([
                        'listing_id' => $listing->id,
                        'is_fba' => $channel === FulfillmentChannelEnum::FBA->value,
                        'event_time' => $notification->event_time,
                    ]);
                }
                $offerSummary[$channel]->update($toUpdate);
            }
        }

        //keep track of old and new 'is_own_offer' offer prices to record in BuyboxActivity table
        $ownOldPrices = Offer::ownOfferPricesByFulfillmentChannel($listing);
        $ownOldBuybox = Offer::ownBuyboxByFulfillmentChannel($listing);

        $ownNewPrices = [];
        $ownNewBuybox = array_fill_keys([FulfillmentChannelEnum::FBA->value, FulfillmentChannelEnum::MERCHANT->value], false);

        // its easier to just delete all current offers for this listing and create new ones
        // rather than trying to update offer entries in the DB
        $listing->offers()->delete();

        // keep track of the offer rank just based on the order the offers are listed in the notification
        $rank = array_fill_keys($channels, 1);
        // add the offer data from the notification to the Offers table
        foreach ($data['Offers'] as $offerData) {
            // only process offer data corresponding to condition and subcondition = new
            if (strtolower($offerData['SubCondition']) !== 'new') continue;

            // get relevant data from the notification
            $channel = $offerData['IsFulfilledByAmazon'] ? FulfillmentChannelEnum::FBA->value : FulfillmentChannelEnum::MERCHANT->value;
            $landedPrice = $offerData['ListingPrice']['Amount'] + $offerData['Shipping']['Amount'];
            $isOwnOffer = $offerData['SellerId'] === $listing->seller->merchant_id;
            $isBuyboxWinner = $offerData['IsBuyBoxWinner'] ?? false;
            $isBuyboxEligible = $offerData['IsFeaturedMerchant'] ?? false;

            // as a bonus, this notification type contains seller positive feedback rating data
            // record this data on the Seller if the offer is an is_own_offer
            if ($isOwnOffer && !is_null(Arr::get($offerData, 'SellerFeedbackRating.SellerPositiveFeedbackRating'))) {
                $this->seller->positive_feedback_rating = $offerData['SellerFeedbackRating']['SellerPositiveFeedbackRating'];
                $this->seller->save();
            }

            // check if an offer has already been created for this merchant / listing pair.
            $offer = Offer::where('listing_id', $listing->id)
                            ->where('merchant_id', $offerData['SellerId'])
                            ->where('is_fba', $channel === FulfillmentChannelEnum::FBA->value)
                            ->first();

            // if an offer has not been created for this merchant / listing pair, or if the current landed price is lower
            // than the landed price of the existing offer and that offer is not winning the buybox, we need to create / update offer information
            if (is_null($offer) || (!$offer->is_buybox_winner && ($landedPrice < ($offer->listing_price + $offer->shipping_price)))) {
                // create an offer if there is not an existing one
                if (is_null($offer)) {
                    $offer = Offer::create([
                        'listing_id' => $listing->id,
                        'merchant_id' => $offerData['SellerId'],
                        'listing_price' => $offerData['ListingPrice']['Amount'],
                        'shipping_price' => $offerData['Shipping']['Amount'],
                        'rank' => $rank[$channel],
                        'is_buybox_winner' => $isBuyboxWinner,
                        'is_own_offer' => $isOwnOffer,
                        'is_fba' => $channel === FulfillmentChannelEnum::FBA->value,
                        'is_buybox_eligible' => $isBuyboxEligible,
                        'notification_id' => $notification->id,
                        'seller_feedback_count' => Arr::get($offerData, 'SellerFeedbackRating.FeedbackCount'),
                        'seller_positive_feedback_rating' => Arr::get($offerData, 'SellerFeedbackRating.SellerPositiveFeedbackRating'),
                    ]);
                    // the ranking var needs to be increased every time an offer is created
                    // (but the rank should not be increased if we're just updating an existing
                    // offer for this merchant / listing pair)
                    $rank[$channel] += 1;
                }
                // for a just created offer: add the rest of the offer data
                // for an existing offer: update the offer with the lower prices and update the rest of the fields
                //                        that may be different between the two offers for this merchant / listing pair
                $offer->update([
                    'listing_price' => $offerData['ListingPrice']['Amount'],
                    'shipping_price' => $offerData['Shipping']['Amount'],
                    'is_buybox_eligible' => $isBuyboxEligible,
                    'is_buybox_winner' => $isBuyboxWinner,
                    'ships_from_state' => Arr::get($offerData, 'ShipsFrom.State'),
                    'ships_from_country' => Arr::get($offerData, 'ShipsFrom.Country'),
                    'shipping_maximum_hours' => Arr::get($offerData, 'ShippingTime.MaximumHours'),
                    'shipping_minimum_hours' => Arr::get($offerData, 'ShippingTime.MinimumHours'),
                    'shipping_available_date' => Arr::get($offerData, 'ShippingTime.AvailableDate'),
                    'shipping_availability_type' => Arr::get($offerData, 'ShippingTime.AvailabilityType'),
                    'sub_condition' => Arr::get($offerData, 'SubCondition'),
                    'is_offer_prime' => Arr::get($offerData, 'PrimeInformation.IsOfferPrime'),
                    'is_offer_national_prime' => Arr::get($offerData, 'PrimeInformation.IsOfferNationalPrime'),
                    'ships_domestically' => Arr::get($offerData, 'ShipsDomestically'),
                ]);

                // if the new offer is an own_offer and buybox_winner, track this buybox change and new landed price to record in BuyboxActivity
                if ($isBuyboxWinner && $isOwnOffer) {
                    $ownNewPrices[$channel] = $landedPrice;
                    $ownNewBuybox[$channel] = true;
                }
            }
        }

        // check if buybox changes have occured, and add entries to BuyboxActivity table if so
        // this keeps track of whether the primary seller has offers winning or losing the buybox
        // the buybox essentially corresponds to the top offer (rank = 1)
        $buyboxChanges = array_diff_assoc($ownNewBuybox, $ownOldBuybox);
        if ($buyboxChanges !== []) {
            foreach ($buyboxChanges as $channel => $val) {
                BuyboxActivity::create([
                    'listing_id' => $listing->id,
                    'old_price' => Arr::get($ownOldPrices, $channel),
                    'new_price' => Arr::get($ownNewPrices, $channel),
                    'event' => $val ? BuyboxActivity::EVENT_WON : BuyboxActivity::EVENT_LOST,
                    'is_fba' => $channel === FulfillmentChannelEnum::FBA->value,
                    'event_time' => $notification->event_time
                ]);
            }
        }
    }

    /**
     * Return the offers, offer summaries, and buybox activity corresponding to $this->seller and an asin.
     * This method is used to return data to the controller.
     * 
     * @param string $asin
     * @param DateTime|null $start
     * @param DateTime|null $end
     * @return array
     */
    public function fetchOffers(string $asin, DateTime|null $start = null, DateTime|null $end = null): array
    {
        // get the relevant listing via seller and asin
        $listing = $this->seller->listings()->where('asin', $asin)->first();

        // if there is no record of the listing, attempt to create it
        if (is_null($listing)) {
            $service = new ListingService($this->seller);

            logger()->info("Sleeping for 2 seconds before calling getSingleListing() method.");
            sleep(2);
            $listing = $service->getSingleListing($asin);
            if (is_null($listing)) {
                logger()->error("Could not retrieve listing data for asin `{$asin}` using getPricing(). Returning an error to the user.");
                return [
                    'body' => [
                        'status' => 'error',
                        'message' => 'Could not find any info for the given asin.',
                    ],
                    'code' => 404
                ];
            }
        }

        // parameter validation
        if ((is_null($start) && !is_null($end)) || (is_null($end) && !is_null($start))) {
            logger()->error('Both start and end dates must be specified if one is specified in fetchOffers().');
            return [
                'body' => [
                    'status' => 'error',
                    'message' => 'Invalid buybox key.', // the buyboxKey is how the user sends start and end dates in the API request
                ],
                'code' => 400
            ];
        }
        
        // check if there is data to return, return an error message if there is none
        if ($listing->offers()->count() === 0
            && $listing->offerSummaries()->count() === 0
            && $listing->buyboxActivity()->whereDate('created_at', now()->subWeek())->count() === 0
        ) {
            return [
                'body' => [
                    'status' => 'error',
                    'message' => 'No data found for the given asin.',
                ],
                'code' => 404
            ];
        }

        // handle the buyboxKey, which indicates the time range for returned buybox activity
        $showOnlyUnviewed = true;
        $globalKeyStart = null;
        $globalKeyEnd = null;
        if (!is_null($start) && !is_null($end)) {
            $showOnlyUnviewed = false;
            $globalKeyStart = $start;
            $globalKeyEnd = $end;
        } else {
            $allActivity = $listing->buyboxActivity()
                                    ->where('viewed', false)
                                    ->oldest('event_time')
                                    ->get();
            if (!$allActivity->isEmpty()) {
                $globalKeyStart = $allActivity->first()->event_time;
                $globalKeyEnd = $allActivity->last()->event_time;
            }
        }

        // generate and format the API response
        $returnBody = [
            'status' => 'success',
            'data' => static::formatListingResponse($listing, $start, $end, $showOnlyUnviewed),
        ];

        // add the buyboxKey to the response if one was used when generating this response
        if (!is_null($globalKeyStart) && !is_null($globalKeyEnd)) {
            $buyboxKey = $globalKeyStart->toIso8601ZuluString() . '--' . $globalKeyEnd->toIso8601ZuluString();
            $returnBody['meta']['buyboxKey'] = $buyboxKey;

            $listing->buyboxActivity()
                ->where('event_time', '>=', $globalKeyStart)
                ->where('event_time', '<=', $globalKeyEnd)
                ->update(['viewed' => true]);
        }

        return [
            'body' => $returnBody,
            'code' => 200
        ];
    }

    /**
     * Returns an array with all the formatted data pertaining to a listing, for endpoint responses.
     * $start and $end specify the timeframe for returned buyboxActivity.
     * If $showOnlyUnviewed is true, then all buyboxActivity for the given $listing with 'viewed' equal to false is included.
     * A time period should be specified, or $showOnlyUnviewed should be true.
     * 
     * @param Listing $listing
     * @param DateTime|null $start
     * @param DateTime|null $end
     * @param bool $showOnlyUnviewed
     * @return array
     */
    public static function formatListingResponse(Listing $listing, DateTime|null $start = null, DateTime|null $end = null, bool $showOnlyUnviewed = false): array
    {
        $ownOffers = [
            FulfillmentChannelEnum::FBA->value => $listing->offers()->where('is_fba', true)->where('is_own_offer', true)->get(),
            FulfillmentChannelEnum::MERCHANT->value => $listing->offers()->where('is_fba', false)->where('is_own_offer', true)->get(),
        ];

        $offers = [
            FulfillmentChannelEnum::FBA->value => $listing->offers()->where('is_fba', true)->orderBy('rank')->get(),
            FulfillmentChannelEnum::MERCHANT->value => $listing->offers()->where('is_fba', false)->orderBy('rank')->get(),
        ];

        $summary = [
            FulfillmentChannelEnum::FBA->value => $listing->offerSummaries()->where('is_fba', true)->get(),
            FulfillmentChannelEnum::MERCHANT->value => $listing->offerSummaries()->where('is_fba', false)->get(),
        ];

        if (!is_null($start) && !is_null($end)) {
            $buyboxQ = $listing->buyboxActivity()->where('event_time', '>=', $start)->where('event_time', '<=', $end);
        }

        return [
            'ownOffers' => [
                FulfillmentChannelEnum::FBA->value => !$ownOffers[FulfillmentChannelEnum::FBA->value]->isEmpty()
                        ? OfferResource::collection($ownOffers[FulfillmentChannelEnum::FBA->value])
                        : [],
                FulfillmentChannelEnum::MERCHANT->value => !$ownOffers[FulfillmentChannelEnum::MERCHANT->value]->isEmpty()
                    ? OfferResource::collection($ownOffers[FulfillmentChannelEnum::MERCHANT->value])
                    : [],
            ],
            'offers' => [
                FulfillmentChannelEnum::FBA->value => !$offers[FulfillmentChannelEnum::FBA->value]->isEmpty()
                    ? OfferResource::collection($offers[FulfillmentChannelEnum::FBA->value])
                    : [],
                FulfillmentChannelEnum::MERCHANT->value => !$offers[FulfillmentChannelEnum::MERCHANT->value]->isEmpty()
                    ? OfferResource::collection($offers[FulfillmentChannelEnum::MERCHANT->value])
                    : [],
            ],
            'summary' => [
                FulfillmentChannelEnum::FBA->value => !$summary[FulfillmentChannelEnum::FBA->value]->isEmpty()
                    ? OfferSummaryResource::collection($summary[FulfillmentChannelEnum::FBA->value])
                    : [],
                FulfillmentChannelEnum::MERCHANT->value => !$summary[FulfillmentChannelEnum::MERCHANT->value]->isEmpty()
                    ? OfferSummaryResource::collection($summary[FulfillmentChannelEnum::MERCHANT->value])
                    : [],
            ],
            'salesRanks' => SalesRankResource::collection($listing->salesRanks()->orderBy('rank')->get()),
            'buyboxActivity' => $showOnlyUnviewed
                ? [
                    FulfillmentChannelEnum::FBA->value => !$listing->buyboxActivity()->where('is_fba', true)->where('viewed', false)->get()->isEmpty()
                        ? BuyboxActivityResource::collection($listing->buyboxActivity()->where('is_fba', true)->where('viewed', false)->latest('event_time')->get())
                        : [],
                    FulfillmentChannelEnum::MERCHANT->value => !$listing->buyboxActivity()->where('is_fba', false)->where('viewed', false)->get()->isEmpty()
                        ? BuyboxActivityResource::collection($listing->buyboxActivity()->where('is_fba', false)->where('viewed', false)->latest('event_time')->get())
                        : [],
                ]
                : [
                    FulfillmentChannelEnum::FBA->value => !$buyboxQ->where('is_fba', true)->get()->isEmpty()
                        ? BuyboxActivityResource::collection($buyboxQ->where('is_fba', true)->latest('event_time')->get())
                        : [],
                    FulfillmentChannelEnum::MERCHANT->value => !$buyboxQ->where('is_fba', false)->get()->isEmpty()
                        ? BuyboxActivityResource::collection($buyboxQ->where('is_fba', false)->latest('event_time')->get())
                        : [],
                ],
        ];
    }
}