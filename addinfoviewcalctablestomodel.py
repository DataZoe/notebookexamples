%pip install semantic-link-labs

import sempy.fabric as fabric
import sempy_labs as labs

semanticmodelname = "Direct Lake"

with connect_semantic_model(dataset=semanticmodelname, readonly=False) as tom:
    tom.add_calculated_table(
            name="xColumns",
            expression="INFO.VIEW.COLUMNS()",
            description="Information about the columns in this semantic model."
        )
    tom.add_calculated_table(
            name="xTables",
            expression="INFO.VIEW.TABLES()",
            description="Information about the tables in this semantic model."

        )
    tom.add_calculated_table(
            name="xRelationships",
            expression="INFO.VIEW.RELATIONSHIPS()",
            description="Information about the relationships in this semantic model."
        )
    tom.add_calculated_table(
            name="xMeasures",
            expression="INFO.VIEW.MEASURES()",
            description="Information about the measures in this semantic model."
        )

labs.refresh_semantic_model(dataset=semanticmodelname)
