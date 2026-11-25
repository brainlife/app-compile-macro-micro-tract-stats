[![Abcdspec-compliant](https://img.shields.io/badge/ABCD_Spec-v1.1-green.svg)](https://github.com/brain-life/abcd-spec)
[![Run on Brainlife.io](https://img.shields.io/badge/Brainlife-brainlife.app.397-blue.svg)](https://doi.org/https://doi.org/10.25663/brainlife.app.397)

# Compile tract macro-structural and profile data

<!-- brainlife-app:begin (generated from the App manifest; edit the manifest, not this block) -->

**Merges tract macrostructural statistics (for example from Tractography Quality Check) with tract profile measures into a single tractmeasures CSV per subject, using pandas.**

Reads the macrostructure CSV and the tract profiles CSV, harmonises subject and tract names (removing underscores, hyphens and dots), merges them on subjectID and structureID, and appends the whole-brain (wbfg) row. Use it to prepare one table per subject for group analyses of tract shape and microstructure. Needs a profiles tractmeasures and a macro tractmeasures from the same subject with matching tract names.

Run it on brainlife.io: [https://doi.org/10.25663/brainlife.app.397](https://doi.org/10.25663/brainlife.app.397)

| Input | Datatype | Required | Description |
|---|---|---|---|
| `profiles` | `neuro/tractmeasures` (profiles) | yes | Tract profiles CSV (tractmeasures with node-wise microstructure values per tract). |
| `macro` | `neuro/tractmeasures` (!profiles, !macro_micro) | yes | Tract macrostructure CSV (for example from Tractography Quality Check) with one row per tract and a wbfg whole-brain row. |

| Output | Datatype | Description |
|---|---|---|
| `tractmeasures` | `neuro/tractmeasures` | tractmeasures.csv combining profile and macrostructural measures per tract, plus the whole-brain row. |

Runtime: container `brainlife/freesurfer-stats:1.2`; 4 CPUs.

Please cite:
- Avesani P. et al. The open diffusion data derivatives. Sci Data 6, 69 (2019). [doi:10.1038/s41597-019-0073-y](https://doi.org/10.1038/s41597-019-0073-y)

<!-- brainlife-app:end -->

This app will This app will compile the csv outputs from macrostructural statistics apps like Tractography Quality Check with the profilometry data from Tract Analysis Profiles (micro-stats) into a single csv to ease downstream analyses. This is done using python, specifically nibabel, pandas and numpy.

### Authors

- Brad Caron (bacaron@utexas.edu)

### Contributors

- Soichi Hayashi (shayashi@iu.edu)

### Funding Acknowledgement

brainlife.io is publicly funded and for the sustainability of the project it is helpful to Acknowledge the use of the platform. We kindly ask that you acknowledge the funding below in your publications and code reusing this code.

[![NSF-BCS-1734853](https://img.shields.io/badge/NSF_BCS-1734853-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1734853)
[![NSF-BCS-1636893](https://img.shields.io/badge/NSF_BCS-1636893-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1636893)
[![NSF-ACI-1916518](https://img.shields.io/badge/NSF_ACI-1916518-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1916518)
[![NSF-IIS-1912270](https://img.shields.io/badge/NSF_IIS-1912270-blue.svg)](https://nsf.gov/awardsearch/showAward?AWD_ID=1912270)
[![NIH-NIBIB-R01EB029272](https://img.shields.io/badge/NIH_NIBIB-R01EB029272-green.svg)](https://grantome.com/grant/NIH/R01-EB029272-01)

### Citations

We kindly ask that you cite the following articles when publishing papers and code using this code.

1. Avesani, P., McPherson, B., Hayashi, S. et al. The open diffusion data derivatives, brain data upcycling via integrated publishing of derivatives and reproducible open cloud services. Sci Data 6, 69 (2019). https://doi.org/10.1038/s41597-019-0073-y

#### MIT Copyright (c) 2020 brainlife.io The University of Texas at Austin and Indiana University

## Running the App

### On Brainlife.io

You can submit this App online at [https://doi.org/https://doi.org/10.25663/brainlife.app.397](https://doi.org/https://doi.org/10.25663/brainlife.app.397) via the 'Execute' tab.

### Running Locally (on your machine)

1. git clone this repo

2. Inside the cloned directory, create `config.json` with something like the following content with paths to your input files.

```json
{
	"profiles": "/input/profiles/tractmeasures.nii.gz",
	"macro": "/input/macro/tractmeasures.csv"
}
```

### Sample Datasets

You can download sample datasets from Brainlife using [Brainlife CLI](https://github.com/brain-life/cli).

```
npm install -g brainlife
bl login
mkdir input
bl dataset download
```

3. Launch the App by executing 'main'

```bash
./main
```

## Output

The main output of this App is a tractmeasures datatype containing a csv with macro and micro-structural statistics combined.

#### Product.json

The secondary output of this app is `product.json`. This file allows web interfaces, DB and API calls on the results of the processing.

### Dependencies

This App only requires [singularity](https://www.sylabs.io/singularity/) to run. If you don't have singularity, you will need to install following dependencies.   

- nibabel: https://nipy.org/nibabel/
- numpy: https://numpy.org/
- pandas: https://pandas.pydata.org/

#### MIT Copyright (c) 2020 brainlife.io The University of Texas at Austin and Indiana University
