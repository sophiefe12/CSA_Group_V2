from aws_cdk import (
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    Stack,
    RemovalPolicy,
)
from constructs import Construct

class TranslateStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create S3 bucket
        bucket = s3.Bucket(self, "AudioBucket",
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True)

        # Preprocess task to extract and format the job name
        preprocess_task = sfn.Pass(self, "Preprocess",
            parameters={
                "TranscriptionJobName.$": "States.Format('{}_job', $.requestParameters.key)",
                "OriginalKey.$": "$.requestParameters.key"
            }
        )

        # Transcribe task
        transcribe_task = tasks.CallAwsService(self, "Transcribe",
            service="transcribe",
            action="startTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "$.TranscriptionJobName",
                "Media": {
                    "MediaFileUri.$": "States.Format('s3://{}/{}', '" + bucket.bucket_name + "', $.OriginalKey)"
                },
                "OutputBucketName": bucket.bucket_name,
                "OutputKey.$": "States.Format('transcriptions/{}.json', $.TranscriptionJobName)",
                "IdentifyLanguage": True
            },
            iam_resources=["*"]
        )

        # Check Language Task
        check_language_task = sfn.Choice(self, "Check Language")
        is_english = sfn.Condition.string_equals("$.TranscriptionJob.LanguageCode", "en-US")

        # Translate task
        translate_task = tasks.CallAwsService(self, "Translate",
            service="translate",
            action="translateText",
            parameters={
                "Text.$": "$.TranscriptionJob.Transcript",
                "SourceLanguageCode.$": "$.TranscriptionJob.LanguageCode",
                "TargetLanguageCode": "en"
            },
            iam_resources=["*"]
        )

        # Polly task
        polly_task = tasks.CallAwsService(self, "Polly",
            service="polly",
            action="synthesizeSpeech",
            parameters={
                "OutputFormat": "mp3",
                "Text.$": "$.TranslateText.TranslatedText",
                "VoiceId": "Joanna"
            },
            result_path="$.pollyResult",
            iam_resources=["*"]
        )

        # Save to S3 task
        save_to_s3_task = tasks.CallAwsService(self, "SaveToS3",
            service="s3",
            action="putObject",
            parameters={
                "Bucket": bucket.bucket_name,
                "Key.$": "States.Format('translations/{}.mp3', $.TranscriptionJobName)",
                "Body.$": "$.pollyResult.AudioStream"
            },
            iam_resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"]
        )

        # Define Step Functions Workflow
        workflow_definition = (
            preprocess_task
            .next(transcribe_task)
            .next(check_language_task
                .when(is_english, sfn.Pass(self, "Skip Translation"))
                .otherwise(translate_task.next(polly_task).next(save_to_s3_task))
            )
        )

        # Create Step Functions State Machine
        state_machine = sfn.StateMachine(self, "TranslationStateMachine",
            definition=workflow_definition
        )

        # Attach policies to the state machine role
        state_machine.role.add_to_policy(iam.PolicyStatement(
            actions=["transcribe:StartTranscriptionJob"],
            resources=["*"]
        ))
        state_machine.role.add_to_policy(iam.PolicyStatement(
            actions=["translate:TranslateText"],
            resources=["*"]
        ))
        state_machine.role.add_to_policy(iam.PolicyStatement(
            actions=["polly:SynthesizeSpeech"],
            resources=["*"]
        ))
        state_machine.role.add_to_policy(iam.PolicyStatement(
            actions=["s3:PutObject"],
            resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"]
        ))

        # Grant S3 permissions to Step Functions Role
        bucket.grant_read_write(state_machine.role)

        # Create EventBridge Rule to trigger the Step Functions State Machine
        rule = events.Rule(self, "Rule",
            event_pattern={
                "source": ["aws.s3"],
                "detail_type": ["Object Created"],
                "detail": {
                    "bucket": {
                        "name": [bucket.bucket_name]
                    },
                    "object": {
                        "key": [{
                            "prefix": "uploads/"
                        }]
                    }
                }
            }
        )

        rule.add_target(targets.SfnStateMachine(state_machine))

