#!/usr/bin/env bash
sphinx-apidoc -o rst/ ../
make clean
make html
