<?php

namespace App\Models;

use App\Enums\FulfillmentChannelEnum;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class Offer extends Model
{
    use HasFactory;

    // The attributes of the model
    protected $fillable = [
        'listing_id', // foreign key to listings table
        'merchant_id', // amazon merchant ID
        'listing_price',
        'shipping_price',
        'rank', // the ranking of this offer relative to other offers for the same listing
        'is_own_offer', // bool indicating if this is an offer by the primary user of this application
        'is_fba', // bool indicating if offer is listed as "fulfilled by amazon" 
        'is_buybox_winner', // bool indicating if this offer is the current buybox winner (top rank of all buybox eligible offers)
        'is_buybox_eligible', // bool indicating if this offer is eligible to win the buybox
        'notification_id', // foreign key to notifications table
        'ships_from_state',
        'ships_from_country',
        'shipping_maximum_hours',
        'shipping_minimum_hours',
        'shipping_available_date',
        'shipping_availability_type',
        'sub_condition',
        'is_offer_prime',
        'is_offer_national_prime',
        'ships_domestically',
        'seller_feedback_count',
        'seller_positive_feedback_rating',
    ];

    protected $casts = [
        'listing_price' => 'float',
        'shipping_price' => 'float',
    ];

    /**
     * All of the relationships to be touched.
     * These models' 'updated_at' timestamps will be updated when an Offer is changed.
     *
     * @var array
     */
    protected $touches = [
        'Listing',
    ];

    /**
     * Retrieve the Listing that corresponds to this Offer.
     *
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function listing(): BelongsTo
    {
        return $this->belongsTo(Listing::class);
    }

    /**
     * Retrieve the Notification that corresponds to this Offer.
     *
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function notification(): BelongsTo
    {
        return $this->belongsTo(Notification::class);
    }

    /**
     * Get all own offer prices for the given listing, keyed by fulfillment channel.
     * 
     * @param Listing $listing
     * @return array 
     */
    public static function ownOfferPricesByFulfillmentChannel(Listing $listing): array
    {
        $byChannel = [];

        // get all own offers for the given $listing
        $offers = $listing->offers()->where('is_own_offer', true)->get();

        // fill in $byChannel with the landed price (listing_price + shipping_price) of the own offer for each fulfillment channel
        foreach ($offers as $offer) {
            $key = $offer->is_fba ? FulfillmentChannelEnum::FBA->value : FulfillmentChannelEnum::MERCHANT->value;
            $byChannel[$key] = ($offer->listing_price + $offer->shipping_price);
        }
        return $byChannel;
    }

    /**
     * Return an array indicating that the seller has an offer winning the buybox, keyed by fulfillment channel.
     * 
     * @param Listing $listing
     * @return array 
     */
    public static function ownBuyboxByFulfillmentChannel(Listing $listing): array
    {
        // creates an array like ['fba' => false, 'merchant' => false]
        $byChannel = array_fill_keys([FulfillmentChannelEnum::FBA->value, FulfillmentChannelEnum::MERCHANT->value], false);

        // get all own offers that are also currently winning the buybox for the given $listing
        $offers = $listing->offers()->where('is_own_offer', true)->where('is_buybox_winner', true)->get();

        // if there are own offers winning the buybox,
        // change the respective 'fba' or 'merchant' key in $byChannel to true
        foreach ($offers as $offer) {
            $key = $offer->is_fba ? FulfillmentChannelEnum::FBA->value : FulfillmentChannelEnum::MERCHANT->value;
            $byChannel[$key] = true;
        }
        return $byChannel;
    }
}
