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
    """
}
