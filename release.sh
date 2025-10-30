#!/bin/zsh

# LocalCache Release Script
# Usage: ./release.sh <version>
# Example: ./release.sh 0.2.0

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if version argument is provided
if [ -z "$1" ]; then
    echo "${RED}Error: Version number required${NC}"
    echo "Usage: ./release.sh <version>"
    echo "Example: ./release.sh 0.2.0"
    exit 1
fi

VERSION=$1
BUILD_FILE="java/build.gradle"

echo "${BLUE}════════════════════════════════════════════════════════${NC}"
echo "${BLUE}   LocalCache Release Script${NC}"
echo "${BLUE}   Version: ${VERSION}${NC}"
echo "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""

# Step 1: Check if working directory is clean
echo "${YELLOW}[1/8]${NC} Checking git working directory..."
if [[ -n $(git status -s) ]]; then
    echo "${RED}Error: Working directory is not clean. Commit or stash changes first.${NC}"
    git status -s
    exit 1
fi
echo "${GREEN}✓ Working directory is clean${NC}"
echo ""

# Step 2: Update version in build.gradle
echo "${YELLOW}[2/8]${NC} Updating version in ${BUILD_FILE}..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/^version = '.*'/version = '${VERSION}'/" "${BUILD_FILE}"
else
    # Linux
    sed -i "s/^version = '.*'/version = '${VERSION}'/" "${BUILD_FILE}"
fi
UPDATED_VERSION=$(grep "^version = " "${BUILD_FILE}" | sed "s/version = '\(.*\)'/\1/")
echo "${GREEN}✓ Version updated to: ${UPDATED_VERSION}${NC}"
echo ""

# Step 3: Clean previous builds
echo "${YELLOW}[3/8]${NC} Cleaning previous builds..."
./gradlew :localcache-core:clean
echo "${GREEN}✓ Clean complete${NC}"
echo ""

# Step 4: Run tests
echo "${YELLOW}[4/8]${NC} Running tests..."
./gradlew :localcache-core:test
echo "${GREEN}✓ All tests passed${NC}"
echo ""

# Step 5: Build project
echo "${YELLOW}[5/8]${NC} Building project..."
./gradlew :localcache-core:build
echo "${GREEN}✓ Build successful${NC}"
echo ""

# Step 6: Publish to Maven Central
echo "${YELLOW}[6/8]${NC} Publishing to Maven Central Portal..."
echo "${BLUE}This will sign and upload artifacts automatically${NC}"
./gradlew :localcache-core:publishMavenJavaPublicationToCentralPortal
echo "${GREEN}✓ Published to Maven Central Portal${NC}"
echo ""

# Step 7: Commit and tag
echo "${YELLOW}[7/8]${NC} Committing and tagging release..."
git add "${BUILD_FILE}"
git commit -m "Release v${VERSION}"
git tag -a "v${VERSION}" -m "Release v${VERSION}"
echo "${GREEN}✓ Committed and tagged v${VERSION}${NC}"
echo ""

# Step 8: Push to GitHub
echo "${YELLOW}[8/8]${NC} Pushing to GitHub..."
git push origin main
git push origin "v${VERSION}"
echo "${GREEN}✓ Pushed to GitHub${NC}"
echo ""

# Success summary
echo "${GREEN}════════════════════════════════════════════════════════${NC}"
echo "${GREEN}   ✓ Release v${VERSION} Complete!${NC}"
echo "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo "Next steps:"
echo "  1. Wait ~10 minutes for Maven Central validation"
echo "  2. Check publication status: ${BLUE}https://central.sonatype.com/publishing${NC}"
echo "  3. Create GitHub Release: ${BLUE}https://github.com/pravesh-pandey/LocalCache/releases/new?tag=v${VERSION}${NC}"
echo ""
echo "Your library will be available at:"
echo "  ${BLUE}https://central.sonatype.com/artifact/io.github.pravesh-pandey/localcache/${VERSION}${NC}"
echo ""
echo "${GREEN}🎉 Congratulations!${NC}"
