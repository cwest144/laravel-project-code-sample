This code sample contains selected files from an application I built for a client of Highside Labs'. The application uses Amazon notifications to track the rankings of all of a seller's offers, relative to other offers for the same product listing. The project was built in PHP using Laravel and a postgreSQL database.

Included in this sample are two service classes and two model classes.

The Offer model represents an offer for an Amazon product (identified by an Amazon-unique identifier called an ASIN). An offer can either be from the client for this project (in which case `is_own_offer = true`) or from a competitor. A single Amazon product listing can have many offers, which correspond to the "new and used from..." options a consumer has when buying a product. The primary function of this application is to keep track of the ranking of our client's offers relative to competing offers.

The Notification model represents a notification from Amazon via SQS (simple queue service). Notifications correspond to a notification type, for example `ANY_OFFER_CHANGED` (the offers for a product have changed) and `LISTINGS_ITEM_STATUS_CHANGE` (a listing has been created or deleted).

The NotificationService retrieves and processes notifications from the SQS queue.

The OfferService handles updating the offers for a listing, and returning a formatted response containing relevant offers to the OfferController.