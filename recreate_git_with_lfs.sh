#!/bin/bash
set -e

echo "🔍 Checking prerequisites..."

# Check if git-lfs is installed
if ! command -v git-lfs &> /dev/null; then
    echo "❌ git-lfs is not installed."
    echo "Install it with: brew install git-lfs"
    exit 1
fi

echo "✅ Prerequisites satisfied"
echo ""

# Warn user
echo "⚠️  WARNING: This will completely delete your Git history!"
echo "   - The .git folder will be backed up to .git.backup"
echo "   - A fresh repository will be created"
echo "   - You'll need to force push to overwrite remote"
echo ""
read -p "Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "💾 Step 1: Backing up existing .git folder..."
if [ -d .git ]; then
    rm -rf .git.backup
    mv .git .git.backup
    echo "   Backed up to .git.backup"
else
    echo "   No .git folder found, skipping backup"
fi

echo ""
echo "🗑️  Step 1.5: Removing old database files from .cache/..."
removed_files=()
if [ -f .cache/geo_data.db ]; then
    rm .cache/geo_data.db
    removed_files+=(".cache/geo_data.db")
fi
if [ -f .cache/geo_data.db.orig ]; then
    rm .cache/geo_data.db.orig
    removed_files+=(".cache/geo_data.db.orig")
fi

if [ ${#removed_files[@]} -gt 0 ]; then
    echo "   Removed: ${removed_files[*]}"
    echo "   (Source of truth is data/geo_data.db)"
else
    echo "   No old database files found in .cache/"
fi

echo ""
echo "🆕 Step 2: Initializing new Git repository..."
git init
git branch -M main

echo ""
echo "📦 Step 3: Setting up Git LFS..."
git lfs install

echo ""
echo "📝 Step 4: Tracking *.db files with Git LFS..."
git lfs track "*.db"

echo ""
echo "📝 Step 5: Updating .gitignore..."
# Add .git.backup to gitignore
if ! grep -q "^\.git\.backup" .gitignore; then
    echo ".git.backup" >> .gitignore
    echo "   Added .git.backup to .gitignore"
else
    echo "   .git.backup already in .gitignore"
fi

# Check if exception for data/geo_data.db already exists
if ! grep -q "^!data/geo_data.db" .gitignore; then
    echo "!data/geo_data.db" >> .gitignore
    echo "   Added !data/geo_data.db to .gitignore"
else
    echo "   !data/geo_data.db already in .gitignore"
fi

echo ""
echo "➕ Step 6: Adding all files..."
git add .

echo ""
echo "📊 Step 7: Checking what will be committed..."
echo "----------------------------------------"
git status --short | head -20
if [ $(git status --short | wc -l) -gt 20 ]; then
    echo "... and $(( $(git status --short | wc -l) - 20 )) more files"
fi
echo "----------------------------------------"

echo ""
echo "🔍 Step 8: Verifying LFS tracking..."
echo "Files tracked by LFS:"
git lfs ls-files 2>/dev/null || echo "   (none yet - files will be tracked after commit)"
echo ""
echo "Files that should be tracked by LFS:"
git ls-files | grep '\.db$' || echo "   (no .db files staged)"

echo ""
echo "💾 Step 9: Creating initial commit..."
git commit -m "Initial commit with Git LFS for database files"

echo ""
echo "🔗 Step 10: Adding remote..."
REMOTE_URL="https://github.com/momonala/incognita.git"
git remote add origin "$REMOTE_URL"
echo "   Remote 'origin' added: $REMOTE_URL"

echo ""
echo "✅ Repository recreated successfully!"
echo ""
echo "📤 Next steps:"
echo "   1. Verify LFS is tracking database files:"
echo "      git lfs ls-files"
git lfs ls-files
echo ""
echo "   2. Force push to remote (THIS WILL OVERWRITE REMOTE HISTORY):"
echo "      git push origin main --force"
echo ""
echo "⚠️  Note: Your old .git folder is backed up at .git.backup"
echo "   You can delete it once you've verified everything works:"
echo "   rm -rf .git.backup"
