{{
  config(
    severity = 'warn'
    )
}}
SELECT
    * 
FROM
    {{ source('staging', 'bookings') }}
where
    BOOKING_AMOUNT < 200 
