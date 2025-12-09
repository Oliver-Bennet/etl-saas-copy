FROM public.ecr.aws/lambda/python:3.10.2025.12.05.09

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY handler.py .

CMD [ "handler.lambda_handler" ]
