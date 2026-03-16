FROM python:3.12-slim
WORKDIR /app

# Copy project root (shared package)
COPY shared/ /app/shared/

# Copy this service
COPY deletion_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY deletion_service/ .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
