<?php

namespace App\Services;

use App\Enums\ReportStatusEnum;
use App\Jobs\DownloadReportJob;
use App\Models\Listing;
use App\Models\Notification;
use App\Models\Offer;
use App\Models\Report;
use App\Models\OfferSummary;
use App\Models\Seller;
use App\Models\Subscription;
use Aws\Exception\AwsException;
use Aws\Sqs\SqsClient;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Bus;
use SellingPartnerApi\Api\NotificationsV1Api as NotificationsApi;
use SellingPartnerApi\Api\ReportsV20210630Api;
use SellingPartnerApi\ReportType;
use Throwable;

class NotificationService
{
    private SPAPIService $service;
    private NotificationsApi $api;

    const US_MARKETPLACE_ID = 'ATVPDKIKX0DER';

    public function __construct(private Seller $seller) {
        $this->service = new SPAPIService($seller);
        // this client will be used to interact with the Amazon Selling Partner API
        $this->api = $this->service->makeClient(NotificationsApi::class);
    }

    /**
     * Create an SQS client.
     *
     * @return \Aws\Sqs\SqsClient
     */
    public static function makeSQSClient(): SqsClient
    {
        return new SqsClient([
            'region' => config('services.sqs.region'),
            'version' => '2012-11-05',
            'credentials' => [
                'key' => config('services.sqs.access_key_id'),
                'secret' => config('services.sqs.secret_access_key'),
            ],
        ]);
    }

    /**
     * Fetch notifications from SQS. Returns the number of notifications retrieved, if any.
     * If an error occurs, returns -1.
     *
     * @return int
     */
    public static function fetchSQSNotifications(): int
    {
        // this client is used to fetch notifications from the SQS queue
        $client = static::makeSQSClient();
        $queueUrl = config('services.sqs.url');
        
        try {
            $result = $client->receiveMessage(array(
                'AttributeNames' => ['SentTimestamp'],
                'MaxNumberOfMessages' => 1,
                'MessageAttributeNames' => ['All'],
                'QueueUrl' => $queueUrl,
                'WaitTimeSeconds' => 0,
            ));
            $messages = $result->get('Messages');
            if (empty($messages)) return 0;

            foreach ($messages as $message) {
                dispatch(fn () => static::processNotification(
                    json_decode($message['Body'], true),
                    $message['ReceiptHandle']
                ));
            }
            return count($messages);
        } catch (AwsException $e) {
            logger()->error('Error receiving SQS notifications', [$e]);
            return -1;
        }
    }

    /**
     * Process a notification payload, and delete the message from SQS if it is
     * processed successfully.
     * 
     * @param array $payload
     * @param string $receiptHandle
     * @return void
     */
    public static function processNotification(array $payload, string $receiptHandle): void
    {   
        // the capitalization scheme of keys in the amazon payload is unpredictable,
        // use this function to check $arr for both possible $titleCaseKey formats
        $icaseArrGet = function ($arr, $titleCaseKey) {
            $lcKeys = array_map('lcfirst', explode('.', $titleCaseKey));
            $lcKey = implode('.', $lcKeys);
            return Arr::get($arr, $titleCaseKey) ?? Arr::get($arr, $lcKey);
        };

        $amzSubscriptionId = $icaseArrGet($payload, 'NotificationMetadata.SubscriptionId');

        //some notification types put all the relevant data inside a 'detail' key
        if (is_null($amzSubscriptionId)) {
            $payload = $payload['detail'];
            $amzSubscriptionId = Arr::get($payload, 'NotificationMetadata.SubscriptionId');
        }

        // get the relevant subscription from the Subscriptions table
        $subscription = Subscription::where('amz_subscription_id', $amzSubscriptionId)->first();
        if (is_null($subscription)) {
            $notificationType = $icaseArrGet($payload, 'NotificationType');
            logger()->error("Unknown notification received with subscription id #{$amzSubscriptionId} and notification type {$notificationType}");
            return;
        }

        // record this notification in the Notifications table
        $notification = Notification::create([
            'amz_notification_id' => $icaseArrGet($payload, 'NotificationMetadata.NotificationId'),
            'status' => Notification::STATUS_PROCESSING,
            'subscription_id' => $subscription->id,
            'event_time' => $icaseArrGet($payload, 'EventTime'),
            'payload' => $payload,
        ]);

        $notificationType = $notification->subscription->notification_type;
        $data = $icaseArrGet($payload, 'Payload');

        // processing directive for each supported notification type
        switch ($notificationType) {
            // indicates a new listing or a deleted listing
            case 'LISTINGS_ITEM_STATUS_CHANGE':
                // match this notification with the relevant Seller
                $sellerId = $icaseArrGet($data, 'SellerId');
                $seller = Seller::where('merchant_id', $sellerId)->first();
                if (is_null($seller)) {
                    logger()->error("Unknown amazon seller with id #{$sellerId} associated with incoming {$notificationType} notification.");
                    return;
                }

                $asin = $icaseArrGet($data, 'Asin');
                if (is_null($asin)) {
                    logger()->error("No asin provided in incoming {$notificationType} notification for seller # {$seller->id}. Aborting processing.");
                    return;
                };

                // get the listing that corresponds to this notification
                $listingQ = Listing::where('seller_id', $seller->id)->where('asin', $asin);
                $listing = $listingQ->first();

                $deleted = in_array('DELETED', $data['Status']);
                // if the notification is that a listing has been deleted and there is a listing in the Listings table, delete the $listing and all of its offers
                if ($deleted && !is_null($listing)) {
                    //deleting a listing also deletes all of its offers
                    $listingQ->delete();
                //otherwise, create a new Listing if there is not one currently
                } else if (!$deleted && is_null($listing)){
                    Listing::create([
                        'seller_id' => $seller->id,
                        'asin' => $asin
                    ]);
                }
                break;

            // indicates that a report has finished processing and can be fetched
            case 'REPORT_PROCESSING_FINISHED':
                logger()->info("Processing REPORT_PROCESSING_FINISHED notification for report type {$data['reportProcessingFinishedNotification']['reportType']}");
                $data = $data['reportProcessingFinishedNotification'];
                $status = $data['processingStatus'];

                // get the Report that corresponds to this notification
                $report = Report::where('amz_id', $data['reportId'])->first();
                if (is_null($report)) {
                    logger()->error("Could not find report with amazon id {$data['reportId']} in database.");
                    break;
                }
                // if the report has finished processing but doesn't have status DONE, some error occured and we can't process the report
                if ($status !== 'DONE') {
                    logger()->error("Report #{$report->id} finished with status {$status}");
                    $report->status = $status;
                    $report->save();
                    break;
                }
                
                $report->status = ReportStatusEnum::DONE;
                $report->save();

                // depending on the report type configure the method that will be used to process it
                $callbackArr = match ($report->type) {
                    ReportType::GET_MERCHANT_LISTINGS_DATA_LITE['name'] => [new ListingService($report->seller), 'saveListingsData'],
                    ReportType::GET_SELLER_FEEDBACK_DATA['name'] => [new RatingsService(), 'saveRatingsReport']
                };

                // dispatch the job to download the report
                DownloadReportJob::dispatch($report, $callbackArr);
                break;

            // indicates that the offers for a listing have changed
            case 'ANY_OFFER_CHANGED':
                // the structure of this notification requires drilling down one more level
                $data = $data['AnyOfferChangedNotification'];

                // match this notification with the relevant Seller
                $sellerId = $icaseArrGet($data, 'SellerId');
                $seller = Seller::where('merchant_id', $sellerId)->first();
                if (is_null($seller)) {
                    logger()->error("Unknown amazon seller with id {$sellerId} associated with incoming {$notificationType} notification.");
                    return;
                }

                // only process notifications for the US marketplace
                $marketplaceId = $icaseArrGet($data, 'OfferChangeTrigger.MarketplaceId');
                if ($marketplaceId !== static::US_MARKETPLACE_ID) {
                    logger()->info("Aborting processing of {$notificationType} notification because it is for a non-US marketplace with ID: {$marketplaceId}.");
                    break;
                }
                
                // update the offers
                $service = new OfferService($seller);
                $service->updateOffers($data, $notification);
                break;

            // used to keep track of competitive price thresholds for listings
            case 'PRICING_HEALTH':
                // match this notification with the relevant Seller
                $sellerId = $icaseArrGet($data, 'SellerId');
                $seller = Seller::where('merchant_id', $sellerId)->first();
                if (is_null($seller)) {
                    logger()->error("Unknown amazon seller with id {$sellerId} associated with incoming {$notificationType} notification.");
                    return;
                }

                // only process notifications for the US marketplace
                $marketplaceId = $icaseArrGet($data, 'OfferChangeTrigger.MarketplaceId');
                if ($marketplaceId !== static::US_MARKETPLACE_ID) {
                    logger()->info("Aborting processing of {$notificationType} notification because it is for a non-US marketplace with ID: {$marketplaceId}.");
                    break;
                }
                // only process if this is for an offer on a condition new listing
                if (!strtolower($data['merchantOffer']['condition']) === 'new') break;

                $asin = $data['offerChangeTrigger']['asin'];
                
                // this notification is only useful if it contains competitive price threshold information
                if (!isset($data['summary']['referencePrice']['competitivePriceThreshold'])) {
                    logger()->info("Incoming {$notificationType} notification for {$asin} does not contain competitive price information. Aborting processing.");
                    break;
                }
                
                // pull the useful price data from the notification
                $listingPrice = $data['merchantOffer']['listingPrice']['amount'];
                $shippingPrice = $data['merchantOffer']['shipping']['amount'];
                $priceThreshold = $data['summary']['referencePrice']['competitivePriceThreshold']['amount'];

                // get the fulfillment channel (amazon or merchant) that corresponds to this notification
                $isFba = match ($data['merchantOffer']['fulfillmentType']) {
                    'MFN' => false,
                    'MERCHANT' => false,
                    'FBA' => true,
                    'AFN' => true,
                    'AMAZON' => true,
                };

                // get the listing corresponding to this notification
                $listing = Listing::where('seller_id', $seller->id)->where('asin', $asin)->first();

                // if there is not a record of this listing, create a new listing for it
                if (is_null($listing)) {
                    $service = new ListingService($seller);
                    //rate limit for getPricing within getSingleListing is .5 requests /second
                    logger()->info("Sleeping for 2 seconds before calling getSingleListing() method.");
                    sleep(2);
                    $listing = $service->getSingleListing($asin);
                    if (is_null($listing)) {
                        logger()->error("Could not retrieve listing data for asin `{$asin}` using getPricing(). Aborting processing of {$payload['NotificationType']} notification.");
                        return;
                    }
                }

                // get the offer that corresponds to this listing, fulfillment channel, and is the offer by the primary seller
                $offer = $listing->offers->where('is_fba', $isFba)->where('is_own_offer', true)->first();
                // if there are no offers recorded, create a new one that represents the primary seller's offer on this listing (is_own_offer = true)
                if (is_null($offer)) {
                    $offer = new Offer([
                        'listing_id' => $listing->id,
                        'merchant_id' => $seller->merchant_id,
                        'is_own_offer' => true,
                        'is_fba' => $isFba,
                        'is_buybox_winner' => false,
                    ]);
                }
                // get the offerSummary that corresponds to this listing and fulfillment channel
                // the offerSummary contains summary information relevant to all offers on a listing
                $offerSummary = $listing->offerSummaries->where('is_fba', $isFba)->first();
                // create a new offerSummary if there is not one already
                if (is_null($offerSummary)) {
                    $offerSummary = new OfferSummary([
                        'listing_id' => $listing->id,
                        'is_fba' => $isFba,
                        'event_time' => $notification->event_time,
                    ]);
                }

                // update the offer and offerSummary with the price data from this notification
                $offer->listing_price = $listingPrice;
                $offer->shipping_price = $shippingPrice;
                $offerSummary->competitive_price_threshold = $priceThreshold;
                $offer->save();
                $offerSummary->save();
                break;
                
            // notifications of other types are not supported
            default:
                logger()->error("Notification type {$notificationType} not supported.");
                return;
        }

        // update the notification status
        $notification->status = Notification::STATUS_PROCESSED;
        $notification->save();

        // remove this notification from the SQS queue
        $client = static::makeSQSClient();
        $queueUrl = config('services.sqs.url');
        try {
            $client->deleteMessage([
                'QueueUrl' => $queueUrl,
                'ReceiptHandle' => $receiptHandle,
            ]);    
        } catch (AwsException $e) {
            logger()->error("Failed to delete message from SQS queue with ReceiptHandle $receiptHandle and status {$e->getStatusCode()}:", [$e->getMessage()]);
        }
    }

    /**
     * Delete all notifications from the SQS queue.
     * 
     * @return bool
     */
    public static function purgeQueue(): bool
    {
        logger()->info("PURGING SQS QUEUE");
        $client = static::makeSQSClient();
        $queueUrl = config('services.sqs.url');

        try {
            $client->purgeQueue(['QueueUrl' => $queueUrl]);
        } catch (AwsException $e) {
            logger()->error("Error purging sqs queue. Failed with code {$e->getCode()}:", [$e->getMessage()]);
            return false;
        }
        return true;
    }
}
