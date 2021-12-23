# Continuous glucose monitoring (CGM)
Continuous glucose monitoring (CGM) systems provide real-time, dynamic glucose information by tracking interstitial glucose values throughout the day. Glycemic variability, also known as glucose variability, is an established risk factor for hypoglycemia and has been shown to be a risk factor in diabetes complications. Over 20 metrics of glycemic variability have been identified.

This algorithm computes 23 clinically validated glucose variability metrics from CGM data.

We have also developed a [python](https://github.com/DigitalBiomarkerDiscoveryPipeline/cgmquantify) and an [R](https://github.com/DigitalBiomarkerDiscoveryPipeline/cgmquantify-R) package “cgmqantify” that provide functions to calculate glucose summary metrics, glucose variability metrics (as defined in clinical publications), and visualizations to visualize trends in CGM data.

##Acknowledgments
* This work was supported in part by the [Chan Zuckerberg Initiative DAF, an advised fund of Silicon Valley Community Foundation](https://chanzuckerberg.com/eoss/proposals/expanding-the-open-mhealth-platform-to-support-digital-biomarker-discovery/)
    * Grant Number: 2020-218599