FROM python:3.12-slim

WORKDIR /app

# Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App und Konfiguration
COPY app.py .
COPY access/ access/

# Streamlit Konfiguration
RUN mkdir -p /root/.streamlit
RUN echo '[server]\nheadless = true\nport = 8501\n\n[theme]\nbase = "dark"\n' > /root/.streamlit/config.toml

EXPOSE 8501

ENV ACC_SETTINGS=./access/acc_settings.yml

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
