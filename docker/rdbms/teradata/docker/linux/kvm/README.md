# Teradata 17.10 under qemu/KVM in a container

**Bring your own Teradata image.** Nothing from Teradata is included in YADAMU, and none of these
files contains any of it. You need a copy of Teradata's VMware appliance and must accept Teradata's
license terms for it. Teradata no longer offers this image for download; contact Teradata about its
current availability and licence terms. The image we built and tested with (one 200 GB disk,
SLES 12 SP3):

* File: `Teradata_Database_17.10.02.01_SLES12_SP3_on_VMware_20210603130901.ova`
* Size: 4,593,858,560 bytes
* SHA-256 (whole .ova): `383522f5546c40b9b156bd429d82e1fdd82a144b382a0ba15d3bbd01d0502ef1`
* Teradata's manifest inside the .ova (.mf, SHA-1): .ovf `0564efe2cada9f11392a1e024609b794f952a7e2`;
  disk-0.vmdk `85a5bfa6b0a3e1c1e1f774ef877fa5afc83262a6`

Other versions of the appliance may need changes to `makeBase.sh`.

The container runs qemu with `/dev/kvm`. It needs no VirtualBox, no host kernel module and no
`--privileged`.

## Requirements

Building the base (`makeBase.sh`, run once on the docker host):

* An x86_64 Linux host with hardware virtualization and KVM (`/dev/kvm` exists), run as a user in
  the `kvm` group.
* The packages `qemu-system-x86`, `qemu-utils`, `openssh-client` (OpenSSH 8.4 or later, for
  `SSH_ASKPASS_REQUIRE`) and `netcat-openbsd` (for `nc -U`), plus `tar` and `coreutils`. On Ubuntu:
  `sudo apt-get install qemu-system-x86 qemu-utils openssh-client netcat-openbsd`.
* Port 2222 on 127.0.0.1 free while the build runs (the guest's ssh is forwarded there).
* About 8 GB of free memory, and about 25 GB free where the base directory goes (the base itself is
  about 16 GB).

Running the container: Docker with Compose v2 and `/dev/kvm`. The guest uses 8 GB of RAM.

## Layout

| File | Purpose |
|------|---------|
| `makeBase.sh` | One-time build of the base disks from the OVA (runs on the docker host; see Requirements) |
| `Dockerfile`  | Ubuntu with qemu. The image holds no Teradata bits |
| `tdvm.sh`     | Container entrypoint: boots the base disks through per-container overlays |

The base directory holds three qcow2 disks, made read-only, and the ssh key for the guest's root
account. The container mounts it read-only at `/vm/base`. The guest writes to qcow2 overlays in
the container's `/vm/run`. Because of this:

* `docker compose stop` / `start` keeps the data.
* `docker compose down` / `up` gives a fresh database, with no reimport and no re-run of `tdvm-init`.

## Build the base (once per host)

```bash
docker/rdbms/teradata/docker/linux/kvm/makeBase.sh <your copy of the .ova> <base directory>
```

The base directory holds the database built from the appliance. Keep it outside the YADAMU tree.

It:

1. converts the appliance disk to qcow2 and adds the two empty 200 GB disks the database expects;
2. boots the guest and installs `tdvm_key` for root;
3. runs `tdvm-init` (the appliance's first-boot configuration, which builds the database);
4. stops Teradata cleanly and powers the guest off, after saving a copy of `pdemain`;
5. boots a second time, puts `pdemain` back if the first shutdown deleted it (it always has, so far),
   then starts and cleanly stops Teradata;
6. boots the finished base once more, through throwaway overlays as the container does, and checks
   Teradata starts on its own and stops cleanly.

Five things about the appliance that make the build fail unless handled:

* **PUT's TLS certificate expired on 2023-09-05.** `tdvm-init` drives Teradata's PUT tool, and at
  the real date PUT's client rejects the certificate and hangs. `makeBase.sh` sets the guest clock to
  2023-06-01 and gives the guest no outbound network (so NTP can't correct it). PUT is used only for
  configuration, so later boots run at the real date.
* **Prompt order** is time zone, then NTP server, then root password (Enter = the default, `iumb123`).
  The password prompt shows first in the log only because it goes to unbuffered stderr.
* **The PUT client gives up when its stdin reaches end of file.** `tdvm-init` then reports
  "TD Put Process has ended prematurely ... invalid root password" and skips its remaining steps.
  `makeBase.sh` keeps stdin open until `tdvm-init` exits. The PUT client also gives up if PUT's
  services haven't settled since boot and the clock change, so `makeBase.sh` waits for them first.
* **The first shutdown after `tdvm-init` deletes `/usr/pde/bin/pdemain`.** It happened on every build,
  at either clock, even after a clean TPA stop. Later shutdowns don't do it. Without `pdemain`,
  Teradata never starts again ("startpde(): can't find pdemain"; `rpm -V ppde` shows it missing).
  The cause is unknown, so `makeBase.sh` saves a copy and puts it back on the second boot.
* **Stop Teradata before powering off.** `tdvm-init`'s last step forces a TPA restart. Powering off
  while Teradata is starting leaves `/var/opt/teradata/tdtemp/PanicLoopDetected` behind.
  `makeBase.sh` waits for the restart and stops TPA. The container stops TPA on `docker stop`.

## Run

```bash
cd docker/rdbms/teradata
TDVM_BASE=<base directory> docker compose up -d --build
```

The container is healthy once the database accepts logons. `docker compose stop` powers the guest
down cleanly (allow up to 3 minutes). Useful commands:

```bash
docker exec TDATA17-01 tdvm.sh guest pdestate -a
```

```bash
docker exec TDATA17-01 tdvm.sh guest "bteq <<< '.logon 127.0.0.1/dbc,dbc; select * from dbc.dbcinfo;'"
```

The database listens on port 1025, as user `dbc`, password `dbc`. The guest's root password is
`iumb123`. These are the appliance defaults. From the docker host, connect to `127.0.0.1`. From
containers on `YADAMU-NET`, connect to `TDATA17-01`, which is what `tera#1` in
`qa/connections/connections.json` uses.

If Teradata does not come up (the container stays "starting"), check:

```bash
docker exec TDATA17-01 tdvm.sh guest "pdestate -a; ls /var/opt/teradata/tdtemp/PanicLoopDetected; rpm -V ppde | grep missing"
```

`PanicLoopDetected` alone doesn't stop Teradata: it is written after an unclean stop and cleared
after 30 minutes of uptime. A missing `pdemain` does stop it, and means the base has to be rebuilt.

## QA

The regression connections (`tera#1`) use host `TDATA17-01`. The orchestrator
(`docker/orchestrator`) maps that name to `$TERADATA`, the address of the docker host running the
container. Its default, 192.168.1.233, is our test host, so set `TERADATA` to yours.

Before running the suites, install YADAMU's Teradata objects (`src/sql/teradata/INSTALL_YADAMU.sql`), then
create the `t_teradata` database the `teradataDataTypes` suite copies, as `dbc`:

```bash
bteq < docker/rdbms/teradata/testdata/dataTypeTesting.sql
```

`docs/Teradata.md` describes the suites and the compose file that runs them.

## Smoke test

Driver: `teradatasql` 20.0.72, which isn't one of YADAMU's dependencies; `docs/Teradata.md` explains
how to install it (it replaces the deprecated `teradata-nodejs-driver`). As `dbc` (with `bteq`, or any SQL client on port 1025), create two
databases and a small table:

```sql
CREATE DATABASE YADAMU_SRC FROM DBC AS PERM = 200000000;
CREATE DATABASE YADAMU_TGT FROM DBC AS PERM = 200000000;
CREATE TABLE YADAMU_SRC.EMPLOYEES (EMPLOYEE_ID INTEGER NOT NULL PRIMARY KEY, FIRST_NAME VARCHAR(20),
  LAST_NAME VARCHAR(25) NOT NULL, SALARY DECIMAL(8,2), HIRE_DATE DATE, UPDATED_AT TIMESTAMP(6), NOTES CLOB(10000));
INSERT INTO YADAMU_SRC.EMPLOYEES VALUES (100, 'Steven', 'King', 24000.00, DATE '2003-06-17', TIMESTAMP '2026-10-17 10:00:00.123456', 'first row');
INSERT INTO YADAMU_SRC.EMPLOYEES VALUES (101, 'Neena', 'Kochhar', 17000.00, DATE '2005-09-21', TIMESTAMP '2026-10-17 10:00:01.000000', NULL);
INSERT INTO YADAMU_SRC.EMPLOYEES VALUES (102, 'Lex', 'De Haan', NULL, NULL, NULL, 'non-ASCII: café ü');
```

Save this as `teraCopy.json`, with `directory` set to an empty, writable directory. It copies
Teradata `YADAMU_SRC` to a JSON file, then the file back into Teradata `YADAMU_TGT`:

```json
{
  "connections" : {
    "file" : { "file" : { "directory" : "/tmp/teraCopy" } }
  , "tera" : { "teradata" : { "host" : "127.0.0.1", "user" : "dbc", "password" : "dbc", "log" : "0" } }
  }
, "schemas" : {
    "src"  : { "schema" : "YADAMU_SRC" }
  , "tgt"  : { "schema" : "YADAMU_TGT" }
  , "file" : { "owner"  : "YADAMU_SRC" }
  }
, "parameters" : { "MODE" : "DDL_AND_DATA" }
, "jobs" : [
    { "source" : { "connection" : "tera", "schema" : "src"  }, "target" : { "connection" : "file", "schema" : "file" } }
  , { "source" : { "connection" : "file", "schema" : "file" }, "target" : { "connection" : "tera", "schema" : "tgt"  } }
  ]
}
```

Run it from the repository's `src` directory:

```bash
node node/cli/copy.js CONFIG=/path/to/teraCopy.json ENCRYPTION=false
```

Both jobs should report "Operation completed successfully". This query should then return 0:

```sql
SELECT COUNT(*) FROM (SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, SALARY, HIRE_DATE, UPDATED_AT FROM YADAMU_SRC.EMPLOYEES
  MINUS SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, SALARY, HIRE_DATE, UPDATED_AT FROM YADAMU_TGT.EMPLOYEES) x;
```

Commercial license terms: don't publish timings from Teradata runs.

Sizing: the guest has 8 GB of RAM and 4 vCPUs (override with `TDVM_MEM` / `TDVM_CPUS`), and the
container limit is 10 GB.
