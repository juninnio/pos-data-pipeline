{% test is_positive(model, column_name) %}

with validation as (

    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0

)


select * from validation

{% endtest %}