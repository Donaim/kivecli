# Container Upload Process: Comprehensive Study

## Executive Summary

This document provides a detailed analysis of how container images are uploaded and managed in the Kive platform, with a focus on the `kivecli` Command-Line Interface (CLI). The study covers both Singularity containers and archive-based containers, explaining the upload workflows, app creation processes, and the relationship between Docker and Singularity in the Kive ecosystem.

---

## Part 1: Project Overview

### 1.1 kivecli Project

**Purpose:** `kivecli` is a Python-based CLI tool that provides convenient terminal-based access to the Kive platform, a containerized workflow execution and data management system developed by the CFE Lab.

**Key Components:**
- Command-line interface with 16+ main commands
- Authentication management via environment variables
- Direct integration with Kive REST API through `kiveapi`
- Type-safe implementation with Python 3.7+ support

**Main Commands Include:**
- `run` - Execute a pipeline with specified apps and datasets
- `makecontainer` - Upload Singularity containers
- `findcontainers` - Search for available containers
- `upload_dataset` - Upload data files
- `download` - Download pipeline outputs
- `rerun` - Re-execute pipelines with different parameters

**Key Entry Point:** [src/kivecli/__main__.py](../src/kivecli/__main__.py)

### 1.2 Authentication

The `kivecli` tool authenticates with Kive servers using three environment variables:

```bash
export MICALL_KIVE_SERVER=https://kive.example.com
export MICALL_KIVE_USER=myuser
export MICALL_KIVE_PASSWORD=secret
```

**Implementation:** The authentication flow uses the `login()` context manager, which handles CSRF tokens and session management through the `kiveapi` library.

---

## Part 2: Kive Architecture (Backend System)

### 2.1 Repository Structure

The Kive project (located at `./dist/kive/`) is a Django-based web application with the following key components:

```
dist/kive/
├── kive/                  # Main Django settings
├── kive/container/        # Container management module
├── kive/librarian/        # Dataset management
├── kive/pipeline/         # Pipeline orchestration
├── kive/middleware/       # Core middleware
├── api/                   # REST API definitions
└── ... other modules
```

**Key Backend Module:** `kive/container/` - Manages container lifecycle:
- Models: Container, ContainerFamily, ContainerApp
- APIs: RESTful endpoints via Django REST Framework
- Views: Web UI for management
- Utilities: Singularity validation, app creation, archive handling

### 2.2 Core Container Models

#### ContainerFamily
```python
class ContainerFamily(AccessControl):
    name: str                    # Family name (e.g., "bioinformatics-tools")
    description: str             # Long-form description
    git: str                      # Git repository URL
    containers: Relation         # Related Container instances
```

**Purpose:** Groups related container versions together. A family might contain multiple versions (tags) of the same tool or application.

#### Container
```python
class Container(AccessControl):
    SIMG = "SIMG"                # Singularity image format
    ZIP = "ZIP"                  # Zip archive format
    TAR = "TAR"                  # Tar archive format
    
    family: ForeignKey[ContainerFamily]  # Parent family
    file: FileField              # The actual container file
    file_type: Choice            # SIMG, ZIP, or TAR
    tag: str                      # Version tag (e.g., "v1.2.0")
    description: str             # Container description
    md5: str                      # File integrity checksum
    parent: ForeignKey[Container] # Parent container (for archives)
    apps: Relation               # Related ContainerApp instances
```

**Constraints:**
- Singularity containers cannot have parent containers
- Archive containers must have a Singularity container parent
- All files must have valid MD5 checksums

#### ContainerApp
Represents a single application within a container, defined by:
- `name`: Application name
- `description`: Help/documentation text
- `threads`: Number of CPU threads required
- `memory`: Memory requirement in MB
- `inputs`: Input file specification
- `outputs`: Output file specification

### 2.3 File Type Support

The Kive container system supports three file formats:

| Format | Extension | Purpose | Parent Required | Use Case |
|--------|-----------|---------|-----------------|----------|
| SIMG | `.simg` | Singularity container image | No | Direct container execution with embedded apps |
| ZIP | `.zip` | Archive of driver scripts | Yes | Procedural workflows with shell/Python scripts |
| TAR | `.tar` | Archive of driver scripts | Yes | Similar to ZIP, alternative format |

---

## Part 3: Container Upload Workflows

### 3.1 Singularity Container Upload Process

#### 3.1.1 Overview

Singularity containers are the primary container type in Kive. They are validated single-file uploads that contain a complete runtime environment plus optional "apps" (named entry points).

#### 3.1.2 Upload Flow

```
User invokes: kivecli makecontainer
    ↓
Parse CLI arguments
    ├── --family: Target container family (name or ID)
    ├── --image: Path to .simg file
    ├── --tag: Version tag (typically semantic version)
    ├── --description: Optional metadata
    ├── --users: List of usernames for access grant
    └── --groups: List of group names for access grant
    ↓
Validate inputs (Container.create in container.py)
    ├── Check file exists and is readable
    ├── Check file is regular file (not directory)
    ├── Verify at least one user or group specified
    └── Resolve family name/ID to ContainerFamily object
    ↓
Open file and prepare upload
    ├── Read file as binary stream
    ├── Build metadata dictionary:
    │   ├── family: Family URL reference
    │   ├── tag: Version tag
    │   ├── description: User description
    │   ├── users_allowed: Granted usernames
    │   └── groups_allowed: Granted groups
    └── Issue multipart/form-data POST request
    ↓
Server-side validation (Kive Django backend)
    ├── Validate Singularity format via: singularity inspect <file>
    ├── Extract and parse deffile section
    ├── Create Container model instance
    ├── Compute MD5 checksum
    └── Store file in media storage (MEDIA_ROOT)
    ↓
Auto-create apps from container deffile
    ├── Issue GET request to /api/containers/{id}/content/
    ├── Backend parses deffile to extract apps
    ├── For each valid app found:
    │   ├── Extract metadata from %applabels
    │   ├── Validate required fields (numthreads, memory, io_args)
    │   └── POST to /api/containerapps/ to create app
    └── Log creation status
    ↓
Return container ID to user
```

**Code References:**
- CLI Handler: [src/kivecli/makecontainer.py](../src/kivecli/makecontainer.py)
- Core Logic: [src/kivecli/container.py::Container.create()](../src/kivecli/container.py#L329)
- Validation: [src/kivecli/container.py::_validate_container_upload()](../src/kivecli/container.py#L67)

#### 3.1.3 Example Usage

```bash
# Simple upload with one user
kivecli makecontainer \
  --family bioinformatics-tools \
  --image ./my-tool.simg \
  --tag v1.0.0 \
  --description "My bioinformatics tool"
  --users admin

# Upload with group access
kivecli makecontainer \
  --family bioinformatics-tools \
  --image ./my-tool.simg \
  --tag v1.0.0 \
  --users alice bob \
  --groups developers analysts

# Get JSON output
kivecli makecontainer \
  --family mylab \
  --image ./new-app.simg \
  --tag latest \
  --users admin \
  --json
```

### 3.2 Archive Container Upload

#### 3.2.1 Overview

Archive containers (ZIP or TAR format) provide an alternative to Singularity for defining containerized workflows. They consist of:
- **Parent Singularity container**: Provides the runtime environment
- **Driver scripts**: Shell or Python scripts that implement the actual workflow steps

#### 3.2.2 Archive Structure

Archive containers must follow this directory structure:

```
archive.zip/
├── driver1.sh          # Entry point scripts (must start with #!)
├── driver2.sh
├── lib/
│   ├── helper.sh       # Optional shared libraries
│   └── config.txt
└── kive/               # Reserved directory for Kive metadata
    └── pipeline/       # Pipeline definitions (created by Kive)
        └── pipeline.json
```

**Naming Convention:** Driver scripts must adhere to:
- Filename ends with `.sh` extension or other recognized interpreter suffix
- First line is shebang (e.g., `#!/bin/bash`, `#!/usr/bin/env python3`)
- At least one driver file present

**Validation:** Files not starting with `#!` are rejected as invalid drivers.

#### 3.2.3 Upload Constraints

Unlike Singularity containers, archive containers:
- **MUST** have a valid `parent` field pointing to an SIMG container
- **CANNOT** be uploaded without specifying the parent
- **MUST** contain at least one driver file
- Are executed within the parent container's runtime environment

#### 3.2.4 Execution Context

When an archive container is executed:
1. The parent Singularity container provides the base runtime
2. The archive contents are extracted into the container's filesystem
3. Driver scripts execute within the container context
4. Output files are captured and stored

---

## Part 4: App Creation from Singularity Deffiles

### 4.1 Deffile Parsing Pipeline

Kive automatically extracts application definitions from Singularity deffiles. The process occurs in two stages:

#### 4.1.1 Stage 1: Client-side (kivecli) Notification

After successful container upload, [src/kivecli/container.py](../src/kivecli/container.py) calls `_create_apps_from_content()` to trigger server-side app creation.

#### 4.1.2 Stage 2: Server-side (Kive) App Extraction

The Kive backend performs the actual parsing:

1. **Fetch Content Endpoint**: Issues `GET /api/containers/{id}/content/`
2. **Parse Deffile**:
   - Extracts `%applabels` sections for app metadata
   - Extracts `%apphelp` for documentation
   - Extracts `%apprun` for execution instructions
3. **Validate Each App**:
   - Required field checks: `numthreads`, `memory`, `io_args`
   - Error message validation
   - Input/output specification validation
4. **Create ContainerApp Records**: 
   - POST requests to `/api/containerapps/` endpoint
   - Metadata includes container reference, name, resource requirements

### 4.2 Deffile Format Requirements

For automatic app creation to succeed, Singularity deffiles must follow this structure:

```singularity
Bootstrap: docker
From: ubuntu:20.04

%labels
    Author Your Name
    Version 1.0

%appinstall myapp
    apt-get update
    apt-get install -y myapp

%applabels myapp
    Version 1.0.0
    numthreads 2
    memory 4096
    io_args input_dir output_dir

%apphelp myapp
    This is my application. 
    It processes files in input_dir and writes to output_dir.

%apprun myapp
    myapp --input $1 --output $2
```

### 4.3 App Metadata Fields

Each app requires:

```json
{
  "appname": "myapp",
  "numthreads": 2,              // CPU threads required
  "memory": 4096,               // Memory in MB
  "io_args": ["input", "output"], // [input_spec, output_spec]
  "helpstring": "Description",  // Extracted from %apphelp
  "runstring": "...",           // Extracted from %apprun
  "labeldict": {...}            // All labels as key-value pairs
}
```

### 4.4 Validation Logic

**Required Fields Check:**
```python
if "numthreads" not in app_info or app_info.get("numthreads") is None:
    skip_app("Missing numthreads field")
if "memory" not in app_info or app_info.get("memory") is None:
    skip_app("Missing memory field")
if "io_args" not in app_info or len(app_info["io_args"]) != 2:
    skip_app("Invalid io_args (must have exactly 2 elements)")
```

**Error Message Propagation:**
```python
if "error_messages" in app_info:
    skip_app(f"App has errors: {error_messages}")
```

---

## Part 5: Docker and Singularity Relationship

### 5.1 Docker vs. Singularity in Kive

**Important Finding:** Kive does NOT support direct Docker image uploads. However, Docker plays an important role:

#### 5.1.1 Singularity Bootstrap from Docker

Singularity containers can be built from Docker images:

```singularity
Bootstrap: docker
From: ubuntu:20.04
# or
From: python:3.9
# or
From: quay.io/biocontainers/samtools
```

When you use `Bootstrap: docker`, Singularity's build system will:
1. Pull the Docker image from Docker Hub or specified registry
2. Convert the Docker layer structure to Singularity format
3. Include the Docker entry point and environment in the Singularity image

#### 5.1.2 Why Docker Images Aren't Directly Uploaded

Several reasons explain why Kive doesn't accept Docker images directly:

1. **Runtime Model Difference**:
   - Docker: requires daemon, inherent isolation overhead
   - Singularity: security-oriented, requires no daemon, better HPC integration

2. **File Format**:
   - Docker: multiple layers + manifest.json (not easily deployed)
   - Singularity: single .simg file (easy deployment at scale)

3. **HPC Compatibility**:
   - Singularity: designed for HPC/supercomputer efficiency
   - Docker: designed for microservices

### 5.2 Recommended Workflow: Docker → Singularity → Kive

For users wanting to containerize applications in Kive:

```
1. Write application code
            ↓
2. Create Dockerfile
   ├─ Define base OS (ubuntu, alpine, etc.)
   ├─ Install dependencies
   ├─ Add application code
   └─ Define entry points
            ↓
3. Build Docker image: docker build -t myapp:v1.0 .
            ↓
4. Create Singularity definition file
   ├─ Bootstrap: docker
   ├─ From: myapp:v1.0 (or from registry)
   ├─ Add %appinstall, %applabels, %apphelp, %apprun
   └─ Include numthreads, memory, io_args metadata
            ↓
5. Build Singularity image: singularity build myapp.simg Singularity
            ↓
6. Create container family: (via web UI or API)
            ↓
7. Upload to Kive: kivecli makecontainer --family myapp --image myapp.simg ...
            ↓
8. Apps auto-created from deffile metadata
            ↓
9. Use in Kive pipelines
```

### 5.3 Docker in Kive Infrastructure

Docker is used within Kive's own infrastructure, but not as a container type:
- Kive backup system uses Docker for database dumps
- Development/testing uses Docker Compose
- Build systems use Docker for consistency

---

## Part 6: Permission and Access Control

### 6.1 Permission Model

Every container inherits from `AccessControl`, providing:

```python
class Container(AccessControl):
    user: User                    # Creator/owner
    users_allowed: Set[User]      # Explicitly granted users
    groups_allowed: Set[Group]    # Explicitly granted groups
```

### 6.2 Access Levels

- **Owner/Creator**: Full access (modify, delete, share)
- **Explicit User Grant**: Read access (can use in pipelines)
- **Explicit Group Grant**: Read access via group membership
- **Public**: If no restrictions set (rare)

### 6.3 Permission Specification During Upload

```bash
# Grant to specific users
--users alice bob charlie

# Grant to groups
--groups developers analysts

# Combine users and groups
--users admin --groups all-staff

# Must specify at least one
--users nobody  # ← Fails: 'nobody' must exist in system
```

### 6.4 Permission Changes After Upload

Currently, permission changes after upload require:
- Web UI modification or
- REST API PATCH requests to update the container's `users_allowed` and `groups_allowed` fields

---

## Part 7: Technical Deep Dive: Upload Implementation

### 7.1 HTTP Request Format

The container upload uses multipart/form-data:

```
POST /api/containers/ HTTP/1.1
Host: kive.example.com
Authorization: ... (CSRF token in headers)
Content-Type: multipart/form-data; boundary=----Boundary

------Boundary
Content-Disposition: form-data; name="family"

/api/containerfamilies/42/
------Boundary
Content-Disposition: form-data; name="tag"

v1.0.0
------Boundary
Content-Disposition: form-data; name="description"

My application version 1
------Boundary
Content-Disposition: form-data; name="users_allowed"

["alice", "bob"]
------Boundary
Content-Disposition: form-data; name="groups_allowed"

["developers"]
------Boundary
Content-Disposition: form-data; name="file"; filename="myapp.simg"
Content-Type: application/octet-stream

[binary file contents]
------Boundary--
```

### 7.2 Client-side Request Construction

[src/kivecli/container.py::Container.create()](../src/kivecli/container.py#L329):

```python
with open(image_path, "rb") as handle:
    container = kive.endpoints.containers.post(
        data={
            "family": family.url.value,
            "tag": tag,
            "description": description,
            "users_allowed": users or [],
            "groups_allowed": groups or [],
        },
        files={"file": handle}
    )
```

### 7.3 Server-side Validation

[dist/kive/kive/container/models.py::Container.clean()](../dist/kive/kive/container/models.py#L352):

```python
def clean(self):
    if self.file_type == Container.SIMG:
        if self.parent is not None:
            raise ValidationError("Singularity cannot have parent")
        
        # Validate with Singularity CLI
        check_output([SINGULARITY_COMMAND, 'inspect', file_path])
    else:
        # Archive validation
        if self.parent is None:
            raise ValidationError("Archives need Singularity parent")
        # Check for drivers in archive
```

### 7.4 MD5 Checksum Computation

After upload, Kive computes MD5 for integrity verification:

```python
def set_md5(self):
    """Compute and set the MD5 checksum of the container file."""
    self.md5 = compute_md5(self.file.name)
```

Used for:
- Verifying file wasn't corrupted during transmission
- Detecting duplicates across uploads
- Audit trail purposes

---

## Part 8: Error Handling and Validation

### 8.1 Common Upload Errors

| Error | Cause | Solution |
|-------|-------|----------|
| "File does not exist" | `--image` path is wrong | Verify path exists |
| "Path is not a file" | `--image` points to directory | Use file path, not directory |
| "Container family not found" | Typo in `--family` name | Check family list: `kivecli findcontainerfamilies` |
| "Invalid Singularity container" | File isn't valid .simg | Run `singularity inspect` locally |
| "Must specify at least one user or group" | No `--users` or `--groups` | Add `--users username` |
| "Multiple container families found" | Family name ambiguous | Use family ID instead |

### 8.2 Deffile Parsing Warnings

During app creation, Kive logs warnings but doesn't fail if:
- Some apps are skipped (missing required fields)
- App names are empty
- Help strings are missing

This allows partial uploads where some apps are invalid.

### 8.3 Graceful Degradation

If app creation fails after container upload:
- Container is still successfully uploaded and usable
- Manual app creation is possible via web UI or API
- Warning messages guide next steps

---

## Part 9: REST API Reference

### 9.1 Container Endpoints

| Endpoint | Method | Purpose | Permission |
|----------|--------|---------|-----------|
| `/api/containers/` | GET | List containers (paginated) | Authenticated |
| `/api/containers/` | POST | Upload new container | Developer |
| `/api/containers/{id}/` | GET | Get container details | Granted/public |
| `/api/containers/{id}/` | PATCH | Update container metadata | Owner |
| `/api/containers/{id}/` | DELETE | Remove container | Owner |
| `/api/containers/{id}/download/` | GET | Download container file | Granted/public |
| `/api/containers/{id}/app_list/` | GET | List container's apps | Granted/public |
| `/api/containers/{id}/content/` | GET | Get archive content | Granted/public |
| `/api/containers/{id}/content/` | PUT | Update archive content | Owner |

### 9.2 Query Filters

Container listings support powerful filtering:

```
/api/containers/?filters[0][key]=family_id&filters[0][val]=42
/api/containers/?filters[0][key]=tag&filters[0][val]=v1
/api/containers/?filters[0][key]=smart&filters[0][val]=bioinformatics
```

### 9.3 Pagination

Default: 200 items per page

```
/api/containers/?page=1&page_size=50
```

### 9.4 Container Family Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/api/containerfamilies/` | List all families |
| `/api/containerfamilies/{id}/containers/` | List containers in family |

---

## Part 10: Comparison with Dataset Upload

For context, container uploads share similar patterns with dataset uploads but with key differences:

### Dataset Upload

```bash
kivecli upload_dataset \
  --file data.csv \
  --name "My Data" \
  --description "Dataset description" \
  --users alice bob
```

**Process:**
1. Validate file exists
2. Compute data format metadata (optional)
3. POST to `/api/datasets/`
4. Returns dataset ID

**No additional post-upload processing**

### Container Upload

```bash
kivecli makecontainer \
  --image app.simg \
  --family mylab \
  --tag v1 \
  --users alice bob
```

**Process:**
1. Validate file exists
2. Resolve family reference
3. POST to `/api/containers/`
4. Validate Singularity format
5. **Auto-create apps** from deffile
6. Returns container ID + app creation status

**Key Difference:** Containers trigger automatic app extraction, whereas datasets do not.

---

## Part 11: Limitations and Future Considerations

### 11.1 Current Limitations

1. **No Docker Direct Upload**: Docker images must be converted to Singularity first
2. **No Streaming Upload**: Entire file must fit in memory during upload
3. **No Resume on Failure**: Partial uploads cannot be resumed
4. **Fixed Container Types**: Only SIMG, ZIP, TAR supported (no OCI, no Apptainer variants)
5. **Deffile Parsing Rigid**: Only specific metadata keywords recognized

### 11.2 Scalability Considerations

- Large containers (>1GB): May timeout on slow networks
- Many apps in deffile: Database insertion may be slow
- Concurrent uploads: Limited by server resource pool
- Storage backend: Currently file-based, not object storage

### 11.3 Security Considerations

- Container files stored with user-readable permissions
- No cryptographic signature verification (relies on MD5 only)
- No sandboxing during deffile parsing
- No rate limiting on uploads

---

## Part 12: Summary and Recommendations

### 12.1 Best Practices

1. **Use Semantic Versioning**: Adopt `v1.0.0` style tags
2. **Embed App Metadata in Deffile**: Always include `%applabels` with required fields
3. **Test Locally First**: Validate with `singularity test` before uploading
4. **Document Apps**: Provide helpful `%apphelp` sections
5. **Monitor Upload Logs**: Check for app creation warnings
6. **Use Group Permissions**: More scalable than individual user grants

### 12.2 Troubleshooting Workflow

If container upload fails:

1. **Check file validity**: `singularity inspect myapp.simg`
2. **Verify family exists**: `kivecli findcontainerfamilies --name mylab`
3. **Check credentials**: Ensure `MICALL_KIVE_*` environment variables set
4. **Review server logs**: Check Kive application logs for detailed errors
5. **Manual upload attempt**: Try via web UI to get additional error context

### 12.3 Performance Optimization

For large-scale container deployments:

1. **Pre-create container families**: Batch create families before uploads
2. **Use bulk app APIs**: Create multiple apps in single batch request
3. **Enable compression**: Use gzip when possible for network transit
4. **Parallelize uploads**: Use job queues for concurrent uploads
5. **Archive selection**: Consider TAR over ZIP for better compatibility

---

## Part 13: Glossary

- **Deffile**: Singularity definition file (recipe for building container)
- **App**: Named entry point within a container with specific resource requirements
- **Family**: Grouping of related container versions
- **Driver**: Executable script in archive container
- **Multipart Upload**: HTTP form data containing both metadata and file binary
- **MD5**: Cryptographic hash for file integrity checking
- **SIMG**: Singularity image format (binary file)
- **TAR**: Tape archive format (uncompressed)
- **ZIP**: ZIP archive format (compressed)

---

## References and Further Reading

- **Singularity Documentation**: https://sylabs.io/guides/latest/user-guide/
- **Kive Main Repository**: https://github.com/cfe-lab/kive
- **kivecli Repository**: https://github.com/Donaim/kivecli
- **REST API Design**: RESTful Conventions used throughout Kive
- **Django REST Framework**: https://www.django-rest-framework.org/

---

## Appendix: Complete Example Workflow

### A.1 Creating and Uploading a Complete Application

```bash
# Step 1: Create Singularity definition file
cat > MyApp.singularity << 'EOF'
Bootstrap: docker
From: ubuntu:20.04

%post
    apt-get update
    apt-get install -y python3 python3-pip
    pip3 install numpy scipy

%environment
    export PATH=/app/bin:$PATH

%appinstall analysis
    mkdir -p /app/bin
    cp /analysis.py /app/bin/analyze

%applabels analysis
    Version 1.0.0
    Author Your Name
    numthreads 4
    memory 8192
    io_args input_file output_file

%apphelp analysis
    Performs statistical analysis on input file.
    Input: Text file with numeric data
    Output: Analysis results in JSON format

%apprun analysis
    python3 /app/bin/analyze $1 $2
EOF

# Step 2: Build Singularity image
singularity build myapp.simg MyApp.singularity

# Step 3: Test locally
singularity run --app analysis myapp.simg input.txt output.json

# Step 4: Create container family (if needed)
# This is typically done via web UI, but can be done via API

# Step 5: Upload to Kive
export MICALL_KIVE_SERVER=https://kive.example.com
export MICALL_KIVE_USER=myuser
export MICALL_KIVE_PASSWORD=mypass

kivecli makecontainer \
  --family statistical-tools \
  --image myapp.simg \
  --tag v1.0.0 \
  --description "Statistical analysis application" \
  --users data-analysts \
  --groups research-team

# Step 6: Verify upload
container_id=<id from previous output>
# Container and its apps are now available in Kive
```

### A.2 Complex Archive Container Example

```bash
# Step 1: Prepare archive structure
mkdir -p myworkflow/lib
mkdir -p myworkflow/kive/pipeline

# Create main drivers
cat > myworkflow/step1.sh << 'EOF'
#!/bin/bash
# Data preprocessing
python3 /lib/preprocess.py "$INPUT_DIR" "$TEMP_DIR"
EOF

cat > myworkflow/step2.sh << 'EOF'
#!/bin/bash
# Analysis
python3 /lib/analyze.py "$TEMP_DIR" "$OUTPUT_DIR"
EOF

# Create shared library
cat > myworkflow/lib/preprocess.py << 'EOF'
import sys
# Preprocessing logic here
EOF

cat > myworkflow/lib/analyze.py << 'EOF'
import sys
# Analysis logic here
EOF

# Step 2: Create zip archive
cd myworkflow && zip -r ../workflow.zip . && cd ..

# Step 3: Get parent Singularity container ID
parent_id=$(kvecli findcontainers --tag v1.0.0 --json | jq '.[0].id')

# Step 4: Upload archive
kivecli makecontainer \
  --family workflow-tools \
  --image workflow.zip \
  --tag workflow-v1 \
  --parent $parent_id \  # This would require API extension as current CLI doesn't support it
  --description "Multi-step workflow" \
  --users all-users
```

---

**Document Version:** 1.0  
**Last Updated:** May 12, 2026  
**Author:** Analysis of Container Upload Architecture
