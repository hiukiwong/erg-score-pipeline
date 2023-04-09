FROM public.ecr.aws/lambda/python:3.9
ARG wd=/var/task/

# Install dependencies
COPY requirements.txt ${wd}
RUN python3.9 -m pip install -r requirements.txt -t "${wd}"

COPY transform.py ${wd}

# Set CMD to handler
CMD ["transform.lambda_handler"]