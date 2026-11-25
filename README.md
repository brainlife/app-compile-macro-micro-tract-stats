<h1 align="center">Compile tract macro-structural and profile data</h1>

<p align="center">
  One table per subject that joins tract macrostructure statistics with tract profile measures, ready for group analyses.
</p>

<p align="center">
  <a href="https://github.com/brainlife/abcd-spec"><img src="https://img.shields.io/badge/ABCD_Spec-v1.1-green.svg" alt="ABCD Spec v1.1"></a>
  <a href="https://doi.org/10.25663/brainlife.app.397"><img src="https://img.shields.io/badge/Brainlife-brainlife.app.397-blue.svg" alt="Run on brainlife.io"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-lightgrey.svg" alt="MIT license"></a>
</p>

## 🧠 At a glance

<!-- brainlife-app:begin (generated from the App manifest; edit the manifest, not this block) -->

> **Merges tract macrostructural statistics (for example from Tractography Quality Check) with tract profile measures into a single tractmeasures CSV per subject, using pandas.**

Reads the macrostructure CSV and the tract profiles CSV, harmonises subject and tract names (removing underscores, hyphens and dots), merges them on subjectID and structureID, and appends the whole-brain (wbfg) row. Use it to prepare one table per subject for group analyses of tract shape and microstructure. Needs a profiles tractmeasures and a macro tractmeasures from the same subject with matching tract names.

| | |
|---|---|
| 📥 **Inputs** | `neuro/tractmeasures` ×2 |
| 📤 **Outputs** | `neuro/tractmeasures` |
| 🐳 **Container** | `brainlife/freesurfer-stats:1.2` |
| ⚙️ **Resources** | 4 CPUs |
| 🔗 **Run it** | [doi.org/10.25663/brainlife.app.397](https://doi.org/10.25663/brainlife.app.397) |

#### 📥 Inputs

| Input | Datatype | Required | Description |
|---|---|---|---|
| `profiles` | `neuro/tractmeasures` (profiles) | yes | Tract profiles CSV (tractmeasures with node-wise microstructure values per tract). |
| `macro` | `neuro/tractmeasures` (!profiles, !macro_micro) | yes | Tract macrostructure CSV (for example from Tractography Quality Check) with one row per tract and a wbfg whole-brain row. |

#### 📤 Outputs

| Output | Datatype | Description |
|---|---|---|
| `tractmeasures` | `neuro/tractmeasures` | tractmeasures.csv combining profile and macrostructural measures per tract, plus the whole-brain row. |

<!-- brainlife-app:end -->

## 🚀 Running the App

The App runs from the [`v1.1`](https://github.com/brainlife/app-compile-macro-micro-tract-stats/tree/v1.1) branch of this repository.

### ☁️ On brainlife.io

Open the App at [doi.org/10.25663/brainlife.app.397](https://doi.org/10.25663/brainlife.app.397) and choose two `neuro/tractmeasures` datasets of the same subject:

- **profiles**: tract profiles (for example from Tract Analysis Profiles), with node-wise microstructure values per tract;
- **macro**: tract macrostructure statistics (for example from Tractography Quality Check), with one row per tract and a `wbfg` whole-brain row.

Press **Execute**. The App has no options. The combined table is archived to your project as a `neuro/tractmeasures` dataset tagged `macro_micro`.

### 💻 Locally

You need [Singularity/Apptainer](https://apptainer.org/); pandas and NumPy come from the `brainlife/freesurfer-stats:1.2` container. Without Singularity, install Python 3 with [pandas](https://pandas.pydata.org/) and [NumPy](https://numpy.org/) and run `compile-macro-micro.py` directly.

1. Clone this repository and check out the App branch (`git checkout v1.1`).
2. Create `config.json` with the paths to the two CSV files and the subject ID (on brainlife.io the subject is read from `_inputs`):

   ```json
   {
       "profiles": "/input/profiles/tractmeasures.csv",
       "macro": "/input/macro/tractmeasures.csv",
       "_inputs": [ { "meta": { "subject": "sub-01" } } ]
   }
   ```

3. Run the App:

   ```bash
   ./main
   ```

If a `tractmeasures.csv` path does not exist, the App falls back to the older file name `output_FiberStats.csv` in the same directory.

### 🧪 Sample data

Download sample datasets from brainlife.io with the [brainlife CLI](https://github.com/brainlife/cli):

```bash
npm install -g brainlife
bl login
mkdir input
bl dataset download <dataset id>
```

## 📦 Outputs

```text
tractmeasures/
└── tractmeasures.csv   profile and macrostructure measures per tract and node, plus the wbfg whole-brain row
```

Before merging, the App strips underscores, hyphens and dots from tract names in the macrostructure table, then joins the two tables on `subjectID` and `structureID`.

## 🩺 Troubleshooting

- **`stats computations failed somewhere. please check logs and derivatives`**: `tractmeasures.csv` was not produced. Check that the tract names in the two inputs match (after removing underscores, hyphens and dots) and read the task log.

## 📚 Citing

> [!TIP]
> If you use this App in your work, please cite the App, the software it builds on and brainlife.io (below). GitHub's **Cite this repository** button gives the App citation in APA and BibTeX.

- Compile tract macro-structural and profile data. brainlife.io App. [doi:10.25663/brainlife.app.397](https://doi.org/10.25663/brainlife.app.397)
- Avesani P. et al. The open diffusion data derivatives. Sci Data 6, 69 (2019). [doi:10.1038/s41597-019-0073-y](https://doi.org/10.1038/s41597-019-0073-y)
- Fischl. FreeSurfer. *NeuroImage* 62:774-781 (2012). [doi:10.1016/j.neuroimage.2012.01.021](https://doi.org/10.1016/j.neuroimage.2012.01.021)
- Hayashi S, Caron BA, Heinsfeld AS, et al. brainlife.io: a decentralized and open-source cloud platform to support neuroscience research. *Nature Methods* 21:809–813 (2024). [doi:10.1038/s41592-024-02237-2](https://doi.org/10.1038/s41592-024-02237-2)

## 👥 Authors

- Brad Caron

**Contributors:** Soichi Hayashi

**Project director:** Franco Pestilli

## 💰 Funding

[![Wellcome-226486/Z/22/Z](https://img.shields.io/badge/Wellcome-226486%2FZ%2F22%2FZ-purple.svg)](https://wellcome.org/)
[![NIH-NINDS-U24NS140384](https://img.shields.io/badge/NIH_NINDS-U24NS140384-green.svg)](https://reporter.nih.gov/project-details/11261037)
[![NIH-NINDS-UM1NS132207](https://img.shields.io/badge/NIH_NINDS-UM1NS132207-green.svg)](https://reporter.nih.gov/project-details/11411328)
[![NIH-NIBIB-R01EB029272](https://img.shields.io/badge/NIH_NIBIB-R01EB029272-green.svg)](https://reporter.nih.gov/project-details/10262925)
[![NIH-NIBIB-R01EB030896](https://img.shields.io/badge/NIH_NIBIB-R01EB030896-green.svg)](https://reporter.nih.gov/project-details/10428625)
[![NIH-NIMH-R01MH126699](https://img.shields.io/badge/NIH_NIMH-R01MH126699-green.svg)](https://reporter.nih.gov/project-details/10460628)
[![NIH-NIDCD-R01DC020088](https://img.shields.io/badge/NIH_NIDCD-R01DC020088-green.svg)](https://reporter.nih.gov/project-details/11140284)
[![NSF-BCS-1734853](https://img.shields.io/badge/NSF_BCS-1734853-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1734853)
[![NSF-BCS-2203524](https://img.shields.io/badge/NSF_BCS-2203524-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2203524)
[![NSF-IIS-1636893](https://img.shields.io/badge/NSF_IIS-1636893-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1636893)
[![NSF-OAC-2148729](https://img.shields.io/badge/NSF_OAC-2148729-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2148729)
[![NSF-IIS-1912270](https://img.shields.io/badge/NSF_IIS-1912270-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1912270)
[![NSF-IIS-2148700](https://img.shields.io/badge/NSF_IIS-2148700-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2148700)
[![NSF-OAC-1916518](https://img.shields.io/badge/NSF_OAC-1916518-blue.svg)](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1916518)

<sub>brainlife.io is publicly funded, and acknowledging that support helps keep it running. This App was developed with support from Wellcome, the U.S. National Institutes of Health (including the BRAIN Initiative BRAIN CONNECTS program: The Axonal Projectome EXchange, U24NS140384, and the Center for Mesoscale Connectomics, UM1NS132207) and the U.S. National Science Foundation.</sub>

## ⚖️ License

[MIT](LICENSE). Copyright (c) 2020 brainlife.io, The University of Texas at Austin and Indiana University.
