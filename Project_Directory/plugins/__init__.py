from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.JobProcessingOperator,
        operators.ToolExtractionOperator,
        operators.DataAnalysisOperator      
    ]
    helpers = [
        helpers.SqlQueries
    ]