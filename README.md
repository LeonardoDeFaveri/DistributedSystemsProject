# Distributed Systems Project

The repository for the DS project.

Authors:

- [Leonardo De Faveri](https://github.com/LeonardoDeFaveri)
- [Lorenzo Fumi](https://github.com/DeeJack)

## Requirements

- Gradle
- Java

## Getting started

- Clone the repository: `git clone https://github.com/LeonardoDeFaveri/DistributedSystemsProject`
- Enter the folder with the terminal: `cd DistributedSystemsProject`
- Run the application with gradle (if gradle is in the environment variables): `gradle run`

## Architectural choices

- Both replicas and clients hold a list of all the replicas in the system:
  - Replicas need to know which are the other replicas
  - Clients pick the replica to contact from that list. Each client has a
    favorite replica and keeps contacting it until it crashes. When that happens,
    it picks another one
- Initially the coordinator is the first replica created
- Replicas keep track of the last time they heard anythig from the coordinator.
HearbetMsg and any other kind of message received from the coordinator makes them
reset this time of last contact

## Running the system
- Start the system with `gradle run`
  Replicas and clients are created
- Press `ENTER` to send a start signal to all hosts:
  - Clients begin sending requests to replicas
  - Replicas begin expecting heartbeat messages (or ay kind of message) from the
  coordinator
  - Coordinator begin sending heartbeat messages to replicas
- Press `ENTER` again to send a stop signal to clients forcing them to stop
producing more requests
- Press `ENTER` a third time to terminate the system
