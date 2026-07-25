# arm64-native BWCE console-log emitter (stand-in for the x86-64-only
# mpandav/demo-rest-app:1.0). Writes BWCE AppNode-format log lines — including
# periodic multiline Java stack traces — to stdout.
FROM python:3.12-alpine
COPY emitter.py /emitter.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "/emitter.py"]
