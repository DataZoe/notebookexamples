%pip install semantic-link-labs

try:
    semanticmodelname = "Direct Lake on OneLake"

    import sempy.fabric as fabric
    import sempy_labs as labs
    from sempy_labs.tom import connect_semantic_model

    with connect_semantic_model(dataset=semanticmodelname, readonly=False) as tom:
        try:
            # Mark as date table
            tom.mark_as_date_table(table_name="date", column_name="Date")
        except Exception as e:
            print(f"Couldn't mark as date table: {e}")

        try:
            # Add relationships   
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
        except Exception as e:
            print(f"Couldn't add relationship: {e}")

        try:
            # Add relationships   
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
        except Exception as e:
            print(f"Couldn't add relationship: {e}")

        try:
            # Add relationships   
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
        except Exception as e:
            print(f"Couldn't add relationship: {e}")

        try:
            # Add measures
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Orders",
                description = "Counts the total number of rows in the Sales table, representing the total number of orders.",
                expression = "COUNTROWS(sales)",
                format_string = "#,0"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")

        try:
            # Add measures
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
        except Exception as e:
            print(f"Couldn't add measure: {e}")

        try:
            # Add measures
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Costs",
                description = "Calculates the total costs from the 'sales' table.",
                expression = "SUM(sales[costs])",
                format_string = "\$#,0;(\$#,0);\$#,0"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")

        try:
            # Add measures        
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Profit",
                description = "Calculates the profit by subtracting costs from sales.",
                expression = "[sales] - [costs]",
                format_string = "\$#,0;(\$#,0);\$#,0"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")
        
        try:
            # Add measures
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Profit Margin",
                description = "Calculates the profit margin by dividing the profit by sales, returning a blank value if the sales are zero.",
                expression = "DIVIDE([profit],[sales],BLANK())",
                format_string = "#,0.00%;-#,0.00%;#,0.00%"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")

        try:
            # Add measures    
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Avg Profit Per Order",
                description = "Calculates the average profit per order by dividing the total profit by the total number of orders.",
                expression = "DIVIDE([profit],[orders],0)",
                format_string = "\$#,0;(\$#,0);\$#,0",
                display_folder = "Avg"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")
        
        try:
            # Add measures
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Avg Sales Per Order",
                description = "Calculates the average sales per order by dividing the total sales by the total number of orders.",
                expression = "DIVIDE([sales],[orders],0)",
                format_string = "\$#,0;(\$#,0);\$#,0",
                display_folder = "Avg"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")

        try:
            # Add measures
            tom.add_measure(
                table_name = "Pick a measure",
                measure_name = "Avg Costs Per Order",
                description = "Calculates the average cost per order by dividing the total costs by the number of orders.",
                expression = "DIVIDE([Costs],[orders],0)",
                format_string = "\$#,0;(\$#,0);\$#,0",
                display_folder = "Avg"
            )
        except Exception as e:
            print(f"Couldn't add measure: {e}")
        
        try:
            # Updating table
            tom.model.Tables['Pick a measure'].set_Description("""All the model measures used to aggregate data. These measures can shown as year-over-year by using this syntax CALCULATE([Measure Name], 'Time intelligence'[Time calculation] = "YOY")""")
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.update_column(
                table_name="Pick a measure",
                column_name="Col1",
                hidden = True
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.set_sort_by_column(
                table_name="Date",
                column_name="Month",
                sort_by_column="MonthOfYear"
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.set_sort_by_column(
                table_name="Date",
                column_name="MonthYear",
                sort_by_column="Monthly"
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.set_sort_by_column(
                table_name="Date",
                column_name="DayOfWeek",
                sort_by_column="DayOfWeekNum"
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.update_column(
                table_name="Date",
                column_name="Date",
                description = "The date for each day.",
                format_string="dd mmm yyyy"
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Updating column
            tom.update_column(
                table_name="Date",
                column_name="Monthly",
                format_string="mmm yyyy"
            )
        except Exception as e:
            print(f"Couldn't update column: {e}")

        try:
            # Adding calc table
            tom.add_calculated_table(
                name="xTables",
                description="This is information about this semantic model's tables.",
                expression="INFO.VIEW.TABLES()"
            )
        except Exception as e:
            print(f"Couldn't add calc table: {e}")

        try:
            # Adding field parameter
            tom.add_field_parameter(
                table_name="Field parameter",
                objects=["[Orders]", "[Sales]", "[Costs]", "[Profit]"],
                object_names=["Orders","Sales","Costs","Profit"]
            )
        except Exception as e:
            print(f"Couldn't add field parameter: {e}")

        try:
            # Adding calc group
            tom.add_calculation_group(
                name="Time intelligence",
                precedence=1
            )
            tom.model.Tables['Time intelligence'].Columns['Name'].set_Name("Time calculation")
            tom.update_column(
                table_name="Time intelligence",
                column_name="Time calculation",
                description="Use with measures & date table for Current: current value, MTD: month to date, QTD: quarter to date, YTD: year to date, PY: prior year, PY MTD, PY QTD, YOY: year over year change, YOY%: YOY as a %"
            )
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
        except Exception as e:
            print(f"Couldn't add calc group: {e}")
            
        try:
            # Adding calc group
            tom.add_hierarchy(
                table_name="Date",
                hierarchy_name="Calendar",
                columns=["Year","Month","Date"]
            )
        except Exception as e:
            print(f"Couldn't add hierarchy: {e}")

    try:
        # Refreshing model
        labs.refresh_semantic_model(dataset=semanticmodelname)
    except Exception as e:
        print(f"Couldn't add hierarchy: {e}")

except Exception as global_e:
    print(f"Error in the workflow: {global_e}")
