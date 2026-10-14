# Dataset provenance — acquired 13 October 2026

Source: [2015 Flight Delays and Cancellations](https://www.kaggle.com/datasets/usdot/flight-delays), published by Department of Transportation. Dataset ID 810, version 1; publisher update 2017-02-09T21:36:00.933Z.

Acquired at 2026-10-13T16:51:32.310626+00:00; inspected at 2026-10-13T16:52:01.121476+00:00. Method: Public Kaggle download endpoint; no credentials required for this retrieval. The [publisher metadata](https://www.kaggle.com/api/v1/datasets/view/usdot/flight-delays) reports **CC0: Public Domain**. Raw CSVs and the archive are excluded from the submission.

Archive: 200,067,268 bytes; SHA-256 `112a41f4e1a5300f570fb0eec5ba5fd11965bcdf67be9431179bb011f3493c14`. ZIP CRC and exact three-file inventory passed before extraction.

| File | Bytes | Parsed records | Width mismatches | SHA-256 |
| --- | --- | --- | --- | --- |
| flights.csv | 592,406,591 | 5,819,079 | 0 | `afa4e42188b113dd69427a3fbd00a4b6979c6235c866f963c2899d18a3403a83` |
| airports.csv | 23,867 | 322 | 0 | `23c4cea98c318225bd923b2068ead8af089845071f79fd9ea0037ba6a61cd4e2` |
| airlines.csv | 359 | 14 | 0 | `ec54f453260c2a46fc76d497ab0eb0a4c13724b566fba8a7ce3bb8ad712acca3` |

Inspection streams CSV records with a quoted-field parser; it does not count physical newline characters. The immutable input manifest is `data/manifests/usdot-2015-v1.json`; each full-run manifest embeds the same provenance and file inspection. HDFS receives those exact three files and the manifest.
