<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

class Notification extends Model
{
    use HasFactory;

    public const STATUS_PROCESSING = 'PROCESSING';
    public const STATUS_PROCESSED = 'PROCESSED';

    // The attributes of the model
    public $fillable = [
        'subscription_id', // a foreign key to a subscriptions table, which contains additional information about the notification type
        'amz_notification_id', // amazon notification id
        'status', // the processing status of the notification in this app
        'event_time', // the date and time of the event on amazon which triggered the notification
        'payload', // the contents of the notification
    ];

    protected $casts = [
        'payload' => 'array',
    ];

    /**
     * Retrieve the Subscription that corresponds to this Notification.
     *
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function subscription(): BelongsTo
    {
        return $this->belongsTo(Subscription::class);
    }

    /**
     * Retrieve the Offers that correspond to this Notification.
     *
     * @return \Illuminate\Database\Eloquent\Relations\HasMany
     */
    public function offers(): HasMany
    {
        return $this->hasMany(Offer::class);
    }
}
