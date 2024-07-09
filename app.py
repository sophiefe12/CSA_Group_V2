#!/usr/bin/env python3
import os

import aws_cdk as cdk

from translate.translate_stack import TranslateStack

app = cdk.App()
TranslateStack(app, "TranslateStack")
app.synth()