{# obt means One Big Table #}

{% set configs = [
    {
        "table" : "AIRBNB.SILVER.SILVER_BOOKINGS",
        "columns" : "silver_bookings.*",
        "alias" : "silver_bookings"
    },
    {
        "table" : "AIRBNB.SILVER.SILVER_LISTINGS",
        "columns" : "silver_listings.HOST_ID, silver_listings.PROPERTY_TYPE, silver_listings.ROOM_TYPE, silver_listings.CITY, silver_listings.COUNTRY, silver_listings.ACCOMMODATES, silver_listings.BEDROOMS, silver_listings.BATHROOMS, silver_listings.PRICE_PER_NIGHT, silver_listings.PRICE_PER_NIGHT_TAG, silver_listings.CREATED_AT AS LISTING_CREATED_AT ",
        "alias" : "silver_listings",
        "join_condition" : "silver_bookings.listing_id = silver_listings.listing_id"
    },
    {
        "table" : "AIRBNB.SILVER.SILVER_HOSTS",
        "columns" : "silver_hosts.HOST_NAME, silver_hosts.HOST_SINCE, silver_hosts.IS_SUPERHOST, silver_hosts.RESPONSE_RATE, silver_hosts.RESPONSE_RATE_QUALITY , silver_hosts.CREATED_AT AS HOST_CREATED_AT",
        "alias" : "silver_hosts",
        "join_condition" : "silver_listings.host_id = silver_hosts.host_id"
    }

] %}


select 
    {% for config in configs %}
        {{ config['columns'] }}{% if not loop.last %},{% endif %}
    {% endfor %}
from
    {% for config in configs %}
    {% if loop.first %}
        {{ config['table'] }} as {{ config['alias'] }}
    {% else %}
        LEFT JOIN {{ config['table'] }} as {{ config['alias'] }}
        ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}




 {# =====>  This look like this but we write it dynamically  <====== #}

{# 
select 
    
        *,
    
        silver_listings.HOST_ID, silver_listings.PROPERTY_TYPE, silver_listings.ROOM_TYPE, silver_listings.CITY, silver_listings.COUNTRY, silver_listings.ACCOMMODATES, silver_listings.BEDROOMS, silver_listings.BATHROOMS, silver_listings.PRICE_PER_NIGHT,silver_listing.PRICE_PER_NIGHT_TAG, silver_listings.CREATED_AT AS LISTING_CREATED_AT ,
    
        silver_hosts.HOST_NAME, silver_hosts.HOST_SINCE, silver_hosts.IS_SUPERHOST, silver_hosts.RESPONSE_RATE, silver_hosts.RESPONSE_RATE_QUALITY , silver_hosts.CREATED_AT AS HOST_CREATED_AT
    
from
    
    
        AIRBNB.SILVER.SILVER_BOOKINGS as silver_bookings
    
    
    
        LEFT JOIN AIRBNB.SILVER.SILVER_LISTINGS as silver_listings
        ON silver_bokings.listing_id = silver_listings.listing_id
        
    
    
        LEFT JOIN AIRBNB.SILVER.SILVER_HOSTS as silver_hosts
        ON silver_listings.host_id = silver_hosts.host_id #}