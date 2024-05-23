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
