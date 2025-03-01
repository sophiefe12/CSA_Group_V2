from aws_cdk import (
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    Duration,
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
            iam_resources=["*"],
            result_path="$.TranscriptionResult"
        )

        # Wait for a few seconds before checking the job status
        wait_task = sfn.Wait(self, "WaitForTranscription",
            time=sfn.WaitTime.duration(Duration.seconds(30))
        )

        # Get Transcription Job status task
        get_transcription_task = tasks.CallAwsService(self, "GetTranscriptionJob",
            service="transcribe",
            action="getTranscriptionJob",
            parameters={
                "TranscriptionJobName.$": "$.TranscriptionJobName"
            },
            iam_resources=["*"],
            result_path="$.GetTranscriptionResult"
        )

        # Check if transcription job is complete
        check_status_task = sfn.Choice(self, "CheckJobStatus")
        is_completed = sfn.Condition.string_equals("$.GetTranscriptionResult.TranscriptionJob.TranscriptionJobStatus", "COMPLETED")
        is_in_progress = sfn.Condition.string_equals("$.GetTranscriptionResult.TranscriptionJob.TranscriptionJobStatus", "IN_PROGRESS")

        # Define a pass state to loop back to wait
        loop_back_pass = sfn.Pass(self, "LoopBackPass")

        # Capture transcription result
        capture_transcription_result = sfn.Pass(self, "CaptureTranscriptionResult",
            parameters={
                "TranscriptionResult.$": "$.GetTranscriptionResult"
            }
        )

        # Check Language Task
        check_language_task = sfn.Choice(self, "Check Language")
        is_english = sfn.Condition.string_equals("$.TranscriptionResult.TranscriptionJob.LanguageCode", "en-US")

        # Translate task
        translate_task = tasks.CallAwsService(self, "Translate",
            service="translate",
            action="translateText",
            parameters={
                "Text.$": "$.TranscriptionResult.TranscriptionJob.Transcript",
                "SourceLanguageCode.$": "$.TranscriptionResult.TranscriptionJob.LanguageCode",
                "TargetLanguageCode": "en"
            },
            iam_resources=["*"],
            result_path="$.TranslationResult"
        )

        # Polly task
        polly_task = tasks.CallAwsService(self, "Polly",
            service="polly",
            action="synthesizeSpeech",
            parameters={
                "OutputFormat": "mp3",
                "Text.$": "$.TranslationResult.TranslatedText",
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
            .next(wait_task)
            .next(get_transcription_task)
            .next(check_status_task
                .when(is_completed, capture_transcription_result
                    .next(check_language_task
                        .when(is_english, sfn.Pass(self, "Skip Translation"))
                        .otherwise(translate_task.next(polly_task).next(save_to_s3_task))
                    )
                )
                .when(is_in_progress, loop_back_pass.next(wait_task))
                .otherwise(sfn.Fail(self, "TranscriptionFailed", error="TranscriptionJobFailed"))
            )
        )

        # Create Step Functions State Machine
        state_machine = sfn.StateMachine(self, "TranslationStateMachine",
            definition=workflow_definition,
            timeout=Duration.minutes(10)  # Adjust the timeout as necessary
        )

        # Attach policies to the state machine role
        state_machine.role.add_to_policy(iam.PolicyStatement(
            actions=["transcribe:StartTranscriptionJob", "transcribe:GetTranscriptionJob"],
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
