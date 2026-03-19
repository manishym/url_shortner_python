FROM python:3.12-slim
WORKDIR /app
COPY e2e_tests/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY e2e_tests/test_e2e.py .
CMD ["pytest"]
