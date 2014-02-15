/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#include "CompressedFileWriter.h"
#include "RecordWriter.h"
#include "Logger.h"

#include <cerrno>
#include <cstring>
#include <cassert>

#include <vector>
#include <algorithm>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace std;
namespace fs = boost::filesystem;

#define REPRIORIZATION_INTERVAL 1000

/** Compression memory requirements from man xz(1) */
static size_t PresetCompressionContextMemorySize[] = {
  3145728, 9437184, 17825792, 33554432, 50331648, 98566144, 98566144,
  195035136, 387973120, 706740224
};

/**
 * Minimum number of bytes initiating on-the-fly compression, compression
 * overhead of compressing everything 20 MiB chunks is negligible. Thus, when
 * when we first start on-the-fly compression at 20 MiB we can always stop
 * on-the-fly compression again, this is useful of another OutputFile
 * receives more traffic.
 */
#define MIN_COMRESS_CHUNCK 20 * 1024 * 1024

namespace mozilla {
namespace telemetry {

/** Context that tracks and assists with intermediate file output */
class RecordWriter::OutputFile
{
public:
  OutputFile(const fs::path &aPath, RecordWriter& aOwner)
   : mOwner(aOwner), mCompressor(nullptr), mRecordsSinceLastReprioritization(0),
     mRawFile(NULL), mCompressedFile(NULL), mUncompressedRawSize(0),
     mUncompressedSize(0), mCompressedSize(0), mPath(aPath.parent_path()),
     mPrefix(aPath.filename().string()), mIsCorrupted(false)
  {
    // Initialized with no raw file or on-the-fly compression
  }

  /** Write a record to file */
  bool Write(const char* aRecord, size_t aLength);

  /** True, if on-the-fly compression can be added */
  bool CanAddCompression() const
  {
    assert(!IsCorrupted());
    return mUncompressedRawSize > MIN_COMRESS_CHUNCK;
  }

  /** Compress existing file and keep on-the-fly compressor */
  bool AddCompressor();

  /** Remove associated compressor */
  bool RemoveCompressor();

  /** True, if this OutputFile has an on-the-fly compressor associated */
  bool HasCompressor()
  {
    assert(!IsCorrupted());
    return mCompressor != nullptr;
  }

  /**
   * True, if this file has experienced fatal errors, and file must be assumed
   * corrupted. If true, don't try to finalize this file, just finalize other
   * files and exit non-zero.
   */
  bool IsCorrupted() const
  {
    return mIsCorrupted;
  }

  /** Compress file, move to mUploadFolder */
  bool Finalize();

  /** Return filter path for this OutputFile */
  const fs::path & Path() const
  {
    return mPath;
  }

  /** Get number of records written since last reprioritization */
  uint32_t RecordsSinceLastReprioritization() const
  {
    assert(!IsCorrupted());
    return mRecordsSinceLastReprioritization;
  }

  /** Reset counter of records written since last reprioritization */
  void ResetReprioritizationRecordCounter()
  {
    assert(!IsCorrupted());
    mRecordsSinceLastReprioritization = 0;
  }

  uint64_t GetUncompressedSize() {
    return mUncompressedSize;
  }

  uint64_t GetCompressedSize() {
    return mCompressedSize;
  }

private:
  /** Record writer that owns this OutputFile */
  RecordWriter& mOwner;

  /** Compression context, if on-the-fly compression is active */
  CompressedFileWriter* mCompressor;

  /** Number of records written since last reprioritization */
  uint32_t  mRecordsSinceLastReprioritization;

  /** File handle for raw file, if not currently compressing */
  FILE* mRawFile;

  /** File handle for compressed file */
  FILE* mCompressedFile;

  /** Size of data current written to mRawFile */
  uint64_t mUncompressedRawSize;

  /** Size of data both in mRawFile and chunks compressed */
  uint64_t mUncompressedSize;

  /** Size of compressed data written */
  uint64_t mCompressedSize;

  /** Filter path string */
  fs::path mPath;

  /** Filename prefix string */
  string mPrefix;

  /** True, if this file is corrupted, ie. failures with undefined outcome */
  bool mIsCorrupted;

  /** Get working folder for this OutputFolder */
  fs::path WorkFolder() const
  {
    return mOwner.WorkFolder() / mPath;
  }

  /** Get path to raw file */
  fs::path RawPath() const
  {
    return WorkFolder() / (mPrefix + ".v2.log");
  }

  /** Get path to compressed file */
  fs::path CompressedPath() const
  {
    return WorkFolder() / (mPrefix + ".v2.log.xz");
  }

  /** Get folder to finished file for upload */
  fs::path UploadFolder() const
  {
    return mOwner.UploadFolder() / mPath;
  }

  /** Get path to the finish file for upload */
  fs::path FinishedPath() const
  {
    return UploadFolder() / (mPrefix + ".v2.log." + mOwner.GetUUID() + ".xz");
  }
};

bool RecordWriter::OutputFile::Write(const char* aRecord, size_t aLength)
{
  // Sanity checking internal state
  assert(!IsCorrupted());
  assert(!(mRawFile && mCompressor));
  assert(mUncompressedRawSize <= mUncompressedSize);

  // Count records written since last time things were re-prioritized
  mRecordsSinceLastReprioritization++;

  // Add to uncompressed size
  mUncompressedSize += aLength;

  // If we have compressor, write to it
  if (mCompressor) {
    assert(mCompressedFile && mUncompressedRawSize == 0);
    // If writing to compressed file fails, we return for clean process abortion
    size_t bytesWritten = 0;
    if (!mCompressor->Write(aRecord, aLength, &bytesWritten)) {
      LOGGER(error) << "compressor write failed";
      mIsCorrupted = true;
      return false;
    }
    mCompressedSize += bytesWritten;
  } else {
    // If raw file is missing we open one
    if (!mRawFile) {
      fs::create_directories(WorkFolder());
      mRawFile = fopen(RawPath().c_str(), "w+");
      if (mRawFile == NULL) {
        LOGGER(error) << "fopen failed, " << strerror(errno);
        return false;
      }
    }

    // Write to raw-file
    if (fwrite(aRecord, 1, aLength, mRawFile) != aLength) {
      LOGGER(error) << "fwrite failed, " << strerror(errno);
      mIsCorrupted = true;
      return false;
    }

    // Remember what we wrote to mRawFile
    mUncompressedRawSize += aLength;
  }

  // Check if we need to rotate
  if (mUncompressedSize > mOwner.MaxUncompressedSize()) {
    // Finalize if uncompressed file size limit is reached
    return Finalize();
  }

  return true;
}

bool RecordWriter::OutputFile::AddCompressor()
{
  // Sanity checking internal state
  assert(!IsCorrupted());
  assert(mRawFile && !mCompressor);
  assert(mUncompressedRawSize <= mUncompressedSize);

  // We don't have a compressed file, create one
  if (!mCompressedFile) {
    mCompressedFile = fopen(CompressedPath().c_str(), "a");
    if (mCompressedFile == NULL) {
      LOGGER(error) << "fopen failed, " << strerror(errno);
      return false;
    }
  }

  // Create compressor
  mCompressor = new CompressedFileWriter();
  if (!mCompressor->Initialize(mCompressedFile, mOwner.CompressionPreset())) {
    LOGGER(error) << "compressor initialization failed";
    mIsCorrupted = true;
    return false;
  }

  // Move to beginning of raw file
  rewind(mRawFile);

  // Create a buffer for reading file
  char* buffer = new char[BUFSIZ];

  // While file isn't at end
  while (!feof(mRawFile)) {
    size_t read = fread(buffer, 1, BUFSIZ, mRawFile);

    // Check for errors
    if (read < BUFSIZ && ferror(mRawFile)) {
      LOGGER(error) << "fread failed, " << strerror(errno);
      mIsCorrupted = true;
      return false;
    }

    // Write to compressor
    size_t bytesWritten = 0;
    if (!mCompressor->Write(buffer, read, &bytesWritten)) {
      LOGGER(error) << "compressor write failed";
      mIsCorrupted = true;
      return false;
    }
    mCompressedSize += bytesWritten;
  }

  // Close raw file
  if (fclose(mRawFile)) {
    LOGGER(error) << "fclose failed, " << strerror(errno);
    mIsCorrupted = true;
    return false;
  }
  mRawFile = NULL;

  // Remove raw file
  if (remove(RawPath().c_str())) {
    LOGGER(warning) << "remove failed, " << strerror(errno);
    // This error is not fatal
  }

  return true;
}

bool RecordWriter::OutputFile::RemoveCompressor()
{
  assert(!IsCorrupted());
  assert(!mRawFile);
  assert(mCompressor && mCompressedFile);

  // Finalize compressor
  size_t bytesWritten = 0;
  if (!mCompressor->Finalize(&bytesWritten)) {
    LOGGER(error) << "compressor finalization failure";
    mIsCorrupted = true;
    return false;
  }
  mCompressedSize += bytesWritten;

  // Delete compressor
  delete mCompressor;
  mCompressor = nullptr;

  return true;
}

bool RecordWriter::OutputFile::Finalize()
{
  assert(!IsCorrupted());

  // Check if there is anything to finalize
  if (!mRawFile && !HasCompressor() && !mCompressedFile) {
    return true;
  }

  // If we have raw file, add a compressor which compresses the file
  if (mRawFile) {
    assert(!HasCompressor());
    if (!AddCompressor()) {
      LOGGER(error) << "failure to add compressor";
      mIsCorrupted = true;
      return false;
    }
  }

  // If we have a compressor remove it
  if (HasCompressor()) {
    if (!RemoveCompressor()) {
      LOGGER(error) << "failure to remove compressor";
      mIsCorrupted = true;
      return false;
    }
  }

  // Close the compressed file
  if (fclose(mCompressedFile)) {
    LOGGER(error) << "fclose failed, " << strerror(errno);
    mIsCorrupted = true;
    return false;
  }
  mCompressedFile = NULL;

  // Create upload folder, so we can move the file there
  fs::create_directories(UploadFolder());

  // Move atomically to the upload folder
  if (rename(CompressedPath().c_str(), FinishedPath().c_str())) {
    LOGGER(error) << "rename failed, " << strerror(errno);
    mIsCorrupted = true;
    return false;
  }

  return true;
}

RecordWriter::RecordWriter(const fs::path &aWorkFolder,
                           const fs::path &aUploadFolder,
                           uint64_t aMaxUncompressedSize,
                           size_t aSoftMemoryLimit, uint32_t aCompressionPreset)
   : mWorkFolder(aWorkFolder), mUploadFolder(aUploadFolder),
     mMaxUncompressedSize(aMaxUncompressedSize), mSoftMemoryLimit(aSoftMemoryLimit),
     mCompressionPreset(aCompressionPreset), mFileMap(),
     mRecordsSinceLastReprioritization(0)
{
  // We don't allow use of extreme flag
  assert(mCompressionPreset <= 9);
}

bool RecordWriter::Write(const fs::path &aPath, const char* aRecord,
                         size_t aLength)
{
  // We've hit the repriorization interval reprioritize on-the-fly compression
  if (mRecordsSinceLastReprioritization++ > REPRIORIZATION_INTERVAL) {
    mRecordsSinceLastReprioritization = 0;
    if (!ReprioritizeCompression()) {
      LOGGER(error) << "compression reprioritization failed";
      return false;
    }
  }

  // OutputFile for this path
  OutputFile* file = nullptr;

  // Lookup in hash table
  auto it = mFileMap.find(aPath.string());
  if (it != mFileMap.end()) {
    file = it->second;
  } else {
    // Create output file
    file = new OutputFile(aPath, *this);
    // Store file for use next time
    mFileMap.insert({aPath.string(), file});
  }
  assert(file);

  // Write record to file
  return file->Write(aRecord, aLength);
}

bool RecordWriter::Finalize()
{
  bool retval = true;

  // Serialize each file
  for (auto it = mFileMap.begin(), end = mFileMap.end(); it != end; ++it) {
    auto file = it->second;

    // Don't try to finalized corrupted files, behavior is undefined
    if (file->IsCorrupted()) {
      LOGGER(error) << "file is corrupted, " << file->Path();
      retval = false;
    } else {
      // Finalize file, compress and move to upload folder
      retval = file->Finalize() && retval;
      mMetrics.mUncompressedSize.mValue += file->GetUncompressedSize();
      mMetrics.mCompressedSize.mValue += file->GetCompressedSize();
      ++mMetrics.mGeneratedFiles.mValue;
    }

    delete file;
  }

  // Release map
  mFileMap.clear();

  return retval;
}

bool RecordWriter::ReprioritizeCompression()
{
  // Create a list of files that can be compressed
  vector<OutputFile*> files;
  for(auto &kv : mFileMap) {
    if (kv.second->CanAddCompression()) {
      files.push_back(kv.second);
    }
  }

  // Sort after mRecordsSinceLastReprioritization
  sort(files.begin(), files.end(), [](const OutputFile* f1,
                                      const OutputFile* f2) {
    return f2->RecordsSinceLastReprioritization() <
           f1->RecordsSinceLastReprioritization();
  });

  // Figure out how many compression contexts we can have
  size_t context_size = PresetCompressionContextMemorySize[mCompressionPreset];
  int contexts = (mSoftMemoryLimit / context_size) - 1;
  if (contexts < 0)
    contexts = 0;

  // Remove compression from bottom most files
  int len = files.size();
  int i;
  for(i = contexts; i < len; i++) {
    if (files[i]->HasCompressor()) {
      if (!files[i]->RemoveCompressor()) {
        LOGGER(error) << "failure to remove compressor";
        return false;
      }
    }
  }

  // Add compression to top most files
  for(i = 0; i < contexts; i++) {
    if (!files[i]->HasCompressor()) {
      if (!files[i]->AddCompressor()) {
        LOGGER(error) << "failure to remove compressor";
        return false;
      }
    }
  }

  // Reset mRecordsSinceLastReprioritization
  for(auto kv : mFileMap) {
    kv.second->ResetReprioritizationRecordCounter();
  }

  return true;
}

void RecordWriter::GetMetrics(message::Message& aMsg)
{
  aMsg.clear_fields();
  ConstructField(aMsg, mMetrics.mGeneratedFiles);
  ConstructField(aMsg, mMetrics.mUncompressedSize);
  ConstructField(aMsg, mMetrics.mCompressedSize);

  mMetrics.mGeneratedFiles.mValue = 0;
  mMetrics.mUncompressedSize.mValue = 0;
  mMetrics.mCompressedSize.mValue = 0;
}

std::string RecordWriter::GetUUID()
{
  boost::uuids::uuid u = boost::uuids::random_generator()();
  string ret = to_string(u);
  ret.erase(std::remove(ret.begin(), ret.end(), '-'), ret.end());
  return ret;
}

} // namespace Telemetry
} // namespace mozilla
