import aws_cdk as core
import aws_cdk.assertions as assertions

from translate.translate_stack import TranslateStack

# example tests. To run these tests, uncomment this file along with the example
# resource in translate/translate_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TranslateStack(app, "translate")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
