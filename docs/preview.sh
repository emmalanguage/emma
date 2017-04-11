#!/bin/bash

echo "Visit your site at http://localhost:4000"

bundle exec jekyll serve --watch --verbose --baseurl '' $@
