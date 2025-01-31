# Configuration file for the Sphinx documentation builder.
#
import os
import sys
# Add src, additional_scripts, and tests directories to sys.path
# Add the 'src' folder to the Python path
sys.path.insert(0, os.path.abspath('../../src'))
sys.path.insert(0, os.path.abspath('../../tests'))
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Geonetwork CSW to EDITO STAC'
copyright = '2024, sam f'
author = 'sam f'
release = '1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',  # For generating docstrings automatically
    'sphinx.ext.napoleon',  # For Google and NumPy style docstrings
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.ifconfig',
    # Add other extensions here if necessary
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
# Suppress full module path in titles
add_module_names = True

# Set default autodoc options
autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'show-inheritance': True,
    'exclude-members': '__weakref__',
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# extensions.append("sphinx_wagtail_theme")
# html_theme = 'sphinx_wagtail_theme'
# Modify theme options for Alabaster
# html_theme_options = {
#     'sidebar_width': '300px',  # Modify sidebar width (if applicable)
#     'page_width': '80%',  # Modify content area width (in percentage)
#     'fixed_sidebar': True,  # If True, the sidebar is fixed and scrolls with the page
#     "code_width": "2000px",  # Width of code blocks
#     "logo": None,
#     "logo_alt": "",
#     "logo_height": 0,
#     "logo_width": 0,
# }
# Change the theme if necessary
html_theme = "sphinx_rtd_theme"  # Use a simpler, widely used theme

# Logo settings
html_theme_options = {
    "logo": "https://emodnet.ec.europa.eu/geonetwork/images/logos/8a7f033d-352f-4ad8-846e-b33efb0e9121.png?random1733234490369",
    "logo_alt": "My Project Logo",
    "logo_height": 59,
    "logo_width": 59,
}
html_show_sphinx = False  # Hide Sphinx theme info
html_show_copyright = True

html_static_path = ['_static']
# Include the custom CSS file
# html_css_files = [
#     'custom.css',  # Add your custom CSS file here
# ]
# print("Custom CSS Files:", html_css_files)
# Napoleon settings for docstring styles
napoleon_google_docstring = True
napoleon_use_param = True
napoleon_use_rtype = True
