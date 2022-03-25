# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../../SphinxSourceCode'))


# -- Project information -----------------------------------------------------

project = 'PALET Wiki Test'
copyright = '2022, ch'
author = 'ch'

# The full version, including alpha/beta/rc tags
release = '1.3'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.napoleon',
    'sphinxcontrib.confluencebuilder',
    'sphinx.ext.githubpages'   
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# Autodoc settings
autodoc_member_order = 'bysource'

# Confluence publish related settings 
confluence_publish = True
confluence_space_key = 'PAL'
# confluence_parent_page = ''
confluence_domain_indices = True
confluence_include_search = True
confluence_use_index = True
confluence_server_pass = '' #add back in before pushing to confluence / remove before pushing to git
# (for Confluence Cloud)
confluence_server_url = 'https://cms-dataconnect.atlassian.net/wiki/'
confluence_server_user = 'charles.hogan@cms.hhs.gov'
confluence_page_hierarchy = True
# confluence_publish_dryrun = True



# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'bizstyle'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []