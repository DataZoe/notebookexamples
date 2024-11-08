# Fabric notebook examples

## Create a lakehouse and populate it with data

- Dimensions: Date, Geo, Product
- Fact: sales_1
- Other: measuregroup

## Create semantic model

- Assumes the lakehouse schema above

Includes the following:
- Table renames
- Relationships between tables
- Measures
    - Format strings added
    - Dynamic format string
    - Descriptions
    - Display folder
- Measure table
- Hidden columns
- Custom formatting on Date and Monthly columns
- Sort by column on Month and DayOfWeek columns
- Mark as date table
- Sort by column on MonthYear
- Calculation group (example from aka.ms/calculationgroup)
- Field parameter
- Calculated table
