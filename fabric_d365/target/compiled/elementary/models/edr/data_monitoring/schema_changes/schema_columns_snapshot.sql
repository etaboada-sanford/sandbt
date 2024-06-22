


    with empty_table as (
            select
            
                
        cast('dummy_string' as varchar(4096)) as column_state_id

,
                
        cast('dummy_string' as varchar(4096)) as full_column_name

,
                
        cast('dummy_string' as varchar(4096)) as full_table_name

,
                
        cast('dummy_string' as varchar(4096)) as column_name

,
                
        cast('dummy_string' as varchar(4096)) as data_type

,
                
        cast (1 as bit) as is_new

,
                cast('2091-02-17' as DATETIME2(6)) as detected_at

,
                cast('2091-02-17' as DATETIME2(6)) as created_at


            )
        select * from empty_table
        where 1 = 0
