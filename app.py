#!/usr/bin/env python3

import aws_cdk as cdk
from aws_ddk_core.cicd import CICDPipelineStack
from aws_ddk_core.config import Config
from ddk_app.ddk_app_stack import DdkApplicationStack
from aws_cdk.pipelines import CodePipelineSource

app = cdk.App()


class ApplicationStage(cdk.Stage):
    def __init__(
        self,
        scope,
        environment_id: str,
        resource_params: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, f"Ddk{environment_id.title()}Application", **kwargs)
        DdkApplicationStack(self, "DataPipeline", environment_id, resource_params)


config = Config()
(
    CICDPipelineStack(
        app,
        id="DdkCodePipeline",
        environment_id="dev",
        pipeline_name="ddk-application-pipeline",
    )
    .add_source_action(
        source_action=CodePipelineSource.git_hub(
            "jonatansmith/aws-black-belt-analytics",
            "main",
            authentication=cdk.SecretValue.secrets_manager(
                "blackbelt-lab-github-jonatansmith"
            ),
        )
    )
    .add_synth_action()
    .build()
    .add_stage("dev", ApplicationStage(app, "dev", env=config.get_env("dev"), resource_params=config.get_env_config("dev").get('resources') ) )
    .add_stage("prd", ApplicationStage(app, "prd", env=config.get_env("prd"), resource_params=config.get_env_config("prd").get('resources') ), manual_approvals=True)
    .synth()
)

app.synth()
