QUERIES = {

    'check_if_table_exists': """
    """,

    'get_column_properites': """
         with get_columns as (
            select
                column_name,
                data_type,
                ordinal_position
            from information_schema.columns 
            where
                table_schema = '{schema_name}'
                and table_name = '{table_name}'
            order by ordinal_position

        ),

        object_construct as (

        select
            json_object(
                ARRAY['column_name', 'data_type'],
                ARRAY[column_name, data_type]
            ) as column_properties
        from get_columns
        )

        select
            column_properties::text as "col_properties"
        from object_construct
        ;    
    """,
    'merge_to_table':"""

        MERGE INTO {schema_name}.{dest_table} as t1
        USING
        (SELECT  * FROM {schema_name}.{table_name}) as t2
        ON t1.{primary_key} = t2.{primary_key}
        WHEN MATCHED  and {update_cond} THEN
        UPDATE SET
        {update_columns}
        WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({values_columns});

    """
}
