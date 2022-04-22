FROM golang:1.17

WORKDIR /hydra

COPY . .

RUN apt-get update && apt-get install vim -y

RUN go build -o hydra main.go

RUN mv hydra /usr/local/bin && chmod +x /usr/local/bin/hydra

ENTRYPOINT ["/bin/bash"]