FROM python:3.10

RUN pip install spotipy

ENV PYTHONPATH /SpotifyDataProject/dags

WORKDIR /SpotifyDataProject/dags

CMD ["python", "spotify_dag.py"]