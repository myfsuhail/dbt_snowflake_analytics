select c_custkey, c_name from {{ source ('raw_schema','customer')}}