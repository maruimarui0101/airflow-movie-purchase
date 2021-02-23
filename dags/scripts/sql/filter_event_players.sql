COPY (
    select *
      from djmax.players
     where special_event = true
)
TO '{{ params.temp_filtered_user_purchase }}' WITH (FORMAT CSV, HEADER);