%pip install semantic-link-labs

def create_semantic_model(semanticmodelname, lakehousename, alreadycreated=False):
    """
    Create the semantic model called semanticmodelname using
    the data in lakehousename. Assumes a specific structure.
    """
    import sempy.fabric as fabric
    import sempy_labs as labs
    from sempy_labs import migration, directlake, admin
    from sempy_labs import lakehouse as lake
    from sempy_labs import report as rep
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs.report import ReportWrapper
    from sempy_labs import ConnectWarehouse
    from sempy_labs import ConnectLakehouse

    ########## create semantic model ##########
    tables = lake.get_lakehouse_tables(lakehouse=lakehousename)
    table_names = tables['Table Name'].tolist()

    if alreadycreated == False:
        directlake.generate_direct_lake_semantic_model(dataset=semanticmodelname, lakehouse=lakehousename, lakehouse_tables=table_names)

    ########## add semantic model things ##########
    with connect_semantic_model(dataset=semanticmodelname, readonly=False) as tom:
        for t in tom.model.Tables:
            for c in t.Columns:
                if c.Name.startswith("RowNumber") == False:
                    c.SourceLineageTag = c.Name
            if t.SourceLineageTag == "[dbo].[date]": 
                t.set_Name("Date")
            if t.SourceLineageTag == "[dbo].[sales_1]": 
                t.set_Name("Sales")
            if t.SourceLineageTag == "[dbo].[geo]": 
                t.set_Name("Geo")
            if t.SourceLineageTag == "[dbo].[measuregroup]": 
                t.set_Name("Pick a measure")
            if t.SourceLineageTag == "[dbo].[product]": 
                t.set_Name("Product")
    with connect_semantic_model(dataset=semanticmodelname, readonly=False) as tom:
        tom.mark_as_date_table(table_name="date", column_name="Date")
        tom.add_relationship(
            from_table="Sales",
            from_column="DateID",
            to_table="Date",
            to_column="DateID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="GeoID",
            to_table="Geo",
            to_column="GeoID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_relationship(
            from_table="Sales",
            from_column="ProductCategoryID",
            to_table="Product",
            to_column="ProductCategoryID",
            from_cardinality="Many",
            to_cardinality="One",
            cross_filtering_behavior="OneDirection",
            security_filtering_behavior="OneDirection",
            rely_on_referential_integrity=False,
            is_active=True
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Orders",
            description = "Counts the total number of rows in the Sales table, representing the total number of orders.",
            expression = "COUNTROWS(sales)",
            format_string = "#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Sales",
            description = "Calculates the total sales by summing all the sales values.",
            expression = "SUM(sales[sales])",
            format_string_expression = f"""
                SWITCH(
                    TRUE(),
                    SELECTEDMEASURE() < 1000,"$#,##0",
                    SELECTEDMEASURE() < 1000000, "$#,##0,.0K",
                    SELECTEDMEASURE() < 1000000000, "$#,##0,,.0M",
                    SELECTEDMEASURE() < 1000000000000,"$#,##0,,,.0B",
                    "$#,##0,,,,.0T"
                )
                """ 
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Costs",
            description = "Calculates the total costs from the 'sales' table.",
            expression = "SUM(sales[costs])",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit",
            description = "Calculates the profit by subtracting costs from sales.",
            expression = "[sales] - [costs]",
            format_string = "\$#,0;(\$#,0);\$#,0"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Profit Margin",
            description = "Calculates the profit margin by dividing the profit by sales, returning a blank value if the sales are zero.",
            expression = "DIVIDE([profit],[sales],BLANK())",
            format_string = "#,0.00%;-#,0.00%;#,0.00%"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Profit Per Order",
            description = "Calculates the average profit per order by dividing the total profit by the total number of orders.",
            expression = "DIVIDE([profit],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Sales Per Order",
            description = "Calculates the average sales per order by dividing the total sales by the total number of orders.",
            expression = "DIVIDE([sales],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.add_measure(
            table_name = "Pick a measure",
            measure_name = "Avg Costs Per Order",
            description = "Calculates the average cost per order by dividing the total costs by the number of orders.",
            expression = "DIVIDE([Costs],[orders],0)",
            format_string = "\$#,0;(\$#,0);\$#,0",
            display_folder = "Avg"
        )
        tom.update_column(
            table_name="Pick a measure",
            column_name="Col1",
            hidden = True
        )
        tom.update_column(
            table_name="Pick a measure",
            column_name="ID",
            hidden = True
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="Month",
            sort_by_column="MonthOfYear"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="MonthYear",
            sort_by_column="Monthly"
        )
        tom.set_sort_by_column(
            table_name="Date",
            column_name="DayOfWeek",
            sort_by_column="DayOfWeekNum"
        )
        tom.update_column(
            table_name="Date",
            column_name="Date",
            format_string="dd mmm yyyy"
        )
        tom.update_column(
            table_name="Date",
            column_name="Monthly",
            format_string="mmm yyyy"
        )
        tom.add_calculated_table(
            name="xTables",
            expression="INFO.VIEW.TABLES()"
        )
        tom.add_field_parameter(
            table_name="Field parameter",
            objects=["[Orders]", "[Sales]", "[Costs]", "[Profit]"],
            object_names=["Orders","Sales","Costs","Profit"]
        )
        tom.add_calculation_group(
            name="Time intelligence",
            precedence=1
        )
        tom.model.Tables['Time intelligence'].Columns['Name'].set_Name("Time calculation")
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="Current",
            expression="SELECTEDMEASURE()",
            ordinal=1
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="MTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESMTD('Date'[Date]))",
            ordinal=2
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="QTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESQTD('Date'[Date]))",
            ordinal=3
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YTD",
            expression="CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))",
            ordinal=4
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY",
            expression="CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('Date'[Date]))",
            ordinal=5
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY MTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "MTD"
                )
            """,
            ordinal=6
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="PY QTD",
            expression= f"""
            CALCULATE(
                SELECTEDMEASURE(),
                SAMEPERIODLASTYEAR('Date'[Date]),
                'Time Intelligence'[Time Calculation] = "QTD"
                )
            """,
            ordinal=7
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY",
            expression= f"""
            SELECTEDMEASURE() -
            CALCULATE(
                SELECTEDMEASURE(),
            'Time Intelligence'[Time Calculation] = "PY"
            )
            """,
            ordinal=8
        )
        tom.add_calculation_item(
            table_name="Time intelligence",
            calculation_item_name="YOY%",
            expression= f"""
            DIVIDE(
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="YOY"
                ),
                CALCULATE(
                    SELECTEDMEASURE(),
                    'Time Intelligence'[Time Calculation]="PY"
                )
            )
            """,
            format_string_expression = f""" "#,##0.0%" """,
            ordinal=9
        )
        tom.add_hierarchy(
            table_name="Date",
            hierarchy_name="Calendar",
            columns=["Year","Month","Date"]
        )

    labs.refresh_semantic_model(dataset=semanticmodelname)
