<p align="center"> <img src="assets/RepliByte%20Logo.png" alt="replibyte logo"/> </p>

<h3 align="center">Seed Your Development Database With Real Data ⚡️</h3>
<p align="center">Replibyte is a blazingly fast tool to seed your databases with your production data while keeping sensitive data safe 🔥</p>

<p align="center">
<a href="https://opensource.org/licenses/MIT"> <img alt="MIT License" src="https://img.shields.io/badge/License-MIT-yellow.svg"> </a>
<img src="https://img.shields.io/badge/stability-stable-green.svg?style=flat-square" alt="stable badge">
<img src="https://img.shields.io/badge/stability-stable-green.svg?style=flat-square" alt="stable badge">
<img src="https://github.com/Qovery/replibyte/actions/workflows/build-and-test.yml/badge.svg?style=flat-square" alt="Build and Tests">
<a href="https://discord.qovery.com"> <img alt="Discord" src="https://img.shields.io/discord/688766934917185556?label=discord&style=flat-square"> </a>
</p>


## Usage

This is a hard FORK!! Hard focus on postgres only!! Hard focus on security!!

Requirements 
+ Focus on subsets rather than whole dumps
+ Handling postgres json and hstore data types
+ CSV data format rather than sql statements
+ Separate dump and restore files

Removed stuff that's clearly not in my requirements
+ website - gone
+ mongodb - gone
+ mysql -- probably gone soon too (if it gets in my way)

Add stuff
+ postgres specific transformers, hstore, json etc
+ optimizations to cope with large petabyte data sets

```shell

# building application
cargo update # retrieve dependencies
cargo clean # clean previous build 
cargo build # new compilation

# testing application
docker-compose -f docker-compose-dev.yml up
cargo test
docker-compose -f docker-compose-dev.yml down

# running application
cargo build --release
cd target/release

replibyte -h
replibyte -c conf.yaml dump create
replibyte -c conf.yaml dump list

# restore
replibyte -c conf.yaml dump restore local -v latest -i postgres -p 5432
# restore latest
replibyte -c conf.yaml dump restore remote -v latest
```