import pdoc
import os

# Generate documentation for all modules in the src directory
src_dir = "src/csw2stac"
modules = [f"src.csw2stac.{os.path.splitext(f)[0]}" for f in os.listdir(src_dir) if f.endswith(".py")]

# Generate documentation for each module
docs = [pdoc.pdoc(module) for module in modules]

# Combine all documentation into one string
combined_docs = "\n\n".join(docs)

# Save the combined documentation to README.md
with open("README2.md", "w") as f:
    f.write(combined_docs)