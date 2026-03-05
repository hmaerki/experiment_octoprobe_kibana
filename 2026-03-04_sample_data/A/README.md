## TODO

* id_run now `2026-02-26_20-54-50-CET`
* id_run new `ch_hans_1 | 2026-02-26_20-54-50-CET`

## Testdata and relations

* Testrun

```json
{
    "testbed_name": "testbed_micropython",
    "testbed_instance": "ch_hans_1",
    "time_start": "2026-02-26_08-36-48-CET",
    "time_end": "2026-02-26_20-52-55-CET",
    "ref_firmware": "https://github.com/micropython/micropython.git~17782",
}
```

* Testgroup (relates to exactly on Testrun)

Testgroups are `RUN-TESTS_STANDARD` or `RUN-PERFBENCH`.

Tentacles are `0c30-ESP32_C3_DEVKIT` or `552b-RPI_PICO2_W`.

```json
{
    "directory_relative": "RUN-PERFBENCH,a@0c30-ESP32_C3_DEVKIT",
    "testgroup": "RUN-PERFBENCH",
    "tentacle": "0c30-ESP32_C3_DEVKIT",
    "testid": "RUN-PERFBENCH,a@0c30-ESP32_C3_DEVKIT",
    "commandline": "run-perfbench.py",
    "tentacle_mcu": "esp32",
    "tentacle_reference": "",
    "time_start": "2026-02-26_13-17-45-CET",
    "time_end": "2026-02-26_13-20-24-CET",

}

* Outcomes (relate to exactly on Testgroup)

```json
"outcomes": [
    {
        "name": "perf_bench/bm_chaos.py",
        "outcome": "passed",
        "text": ""
    },
    {
        "name": "perf_bench/bm_fannkuch.py",
        "outcome": "failed",
        "text": ""
    },
]
```

## Required resulting tables

* summary table

    | Testgroup | Tests passed | Tests failed |
    | - | - | - |
    | RUN-PERFBENCH | 3 | 2 |
    | RUN-TESTS_STANDARD | 12 | 5 |

* failures per group

  * group RUN-PERFBENCH

    | Test | Tentacle 0c30-ESP32_C3_DEVKIT | Tentacle 552b-RPI_PICO2_W |
    | - | - | - |
    | RUN-perf_bench/bm_chaos.py | passed | failed |
    | RUN-TESTS_STANDARD | failed | failed |


