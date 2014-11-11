//
//  LiveDepot.m
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

#import "LiveDepot.h"

#import <GBStorage/GBStorage.h>
#import <GBToolbox/GBToolbox.h>
#import <Reachability/Reachability.h>

CGFloat const kLDDownloadProgressUnknown =                      -1.;

static NSString * const kURLSessionIdentifier =                 @"com.goonbee.LiveDepot.BackgroundSession";
static NSString * const kGBStorageNamespace =                   @"com.goonbee.LiveDepot.GBStorage";

static NSString * const kFilesManifestKey =                     @"FilesManifest";
static NSString * const kDownloadsInProgressManifestKey =       @"DownloadsInProgressManifest";
static NSString * const kFileStatusManifestKey =                @"FileStatusManifest";
static NSString * const kDownloadProgressManifestKey =          @"DownloadProgressManifest";

static NSString * const kDefaultContextName =                   @"DefaultContext";
static NSString * const kFileStorageDirectory =                 @"com.goonbee.LiveDepot.FilesDirectory.Downloaded";
static NSString * const kFileSymlinksDirectory =                @"com.goonbee.LiveDepot.FilesDirectory.Symlinks";

static NSString * const kTaskPayloadFlagCancelledPermanently =  @"taskCancelledPermanently";
static NSString * const kTaskPayloadFlagTimedOut =              @"taskTimedOut";
static NSString * const kTaskPayloadFlagDataStorageFailed =     @"taskDataStorageFailed";

static NSTimeInterval const kRequestTimeoutStandard =           5;
static NSTimeInterval const kRequestTimeoutAfterInit =          10;
static NSTimeInterval const kResourceTotalTimeout =             10800;//3 hours
static NSTimeInterval const kAutomaticRetryingTimeout =         30;

typedef enum : NSUInteger {
    LDTaskFailureReasonUnknown,
    LDTaskFailureReasonFileWritingError,
    LDTaskFailureReasonTimeout,
} LDTaskFailureReason;

typedef enum : NSUInteger {
    LDTaskCancellationDispositionPermanent,
    LDTaskCancellationDispositionTimeout,
} LDTaskCancellationDisposition;

@interface LiveDepot () <NSURLSessionDelegate, NSURLSessionDownloadDelegate>

@property (strong, atomic) NSOperationQueue                     *operationQueue;

@property (strong, nonatomic) NSURLSession                      *urlSession;
@property (strong, nonatomic) GBStorageController               *GBStorage;

@property (strong, nonatomic) NSMapTable                        *fileListUpdateHandlers;
@property (strong, nonatomic) NSMapTable                        *fileUpdateHandlers;
@property (strong, nonatomic) NSMapTable                        *wildcardFileUpdateHandlers;

@property (strong, nonatomic, readonly) NSMutableArray          *filesManifest;
@property (strong, nonatomic, readonly) NSMutableSet            *downloadsInProgressManifest;
@property (strong, nonatomic, readonly) NSMutableDictionary     *fileStatusManifest;
@property (strong, nonatomic, readonly) NSMutableDictionary     *downloadProgressManifest;

@property (strong, nonatomic) NSMutableDictionary               *timeoutHandlers;

@property (strong, nonatomic) NSTimer                           *automaticRetryingTimer;

@property (strong, nonatomic) Reachability                      *reachability;

@end

@interface LDFile (Private)

@property (assign, nonatomic, readwrite) BOOL                   hasSourceListChanged;
@property (assign, nonatomic, readwrite) BOOL                   isDataOutOfDate;

- (NSArray *)_normalizedSources;

@end

@interface NSArray (LiveDepot)

- (BOOL)containsFileWithIdentifier:(NSString *)fileIdentifier;

@end

@implementation LiveDepot

#pragma mark - CA

- (NSMutableArray *)filesManifest {
    return (NSMutableArray *)self.GBStorage[kFilesManifestKey];
}

- (NSMutableDictionary *)fileStatusManifest {
    return (NSMutableDictionary *)self.GBStorage[kFileStatusManifestKey];
}

- (NSMutableDictionary *)downloadProgressManifest {
    return (NSMutableDictionary *)self.GBStorage[kDownloadProgressManifestKey];
}

- (NSMutableSet *)downloadsInProgressManifest {
    return (NSMutableSet *)self.GBStorage[kDownloadsInProgressManifestKey];
}

#pragma mark - API: General

+ (LiveDepot *)sharedDepot {
    static LiveDepot *accessor;
    @synchronized(self) {
        if (!accessor) {
            accessor = [[LiveDepot alloc] _init];
        }
        
        return accessor;
    }
}

+ (void)handleEventsForBackgroundURLSessionWithHandler:(void (^)())completionHandler {
    [LiveDepot sharedDepot].backgroundSessionCompletionHandler = completionHandler;
}

- (id)init {
    @throw [NSException exceptionWithName:NSInternalInconsistencyException reason:@"You cannot create instances of LiveDepot, use the singleton [LiveDepot sharedDepot] instead." userInfo:nil];
}

#pragma mark - API: Files manipulation

- (void)addFile:(LDFile *)file {
    AssertParameterNotNil(file);
    
    [self _addFile:file triggerUpdates:YES commitToDisk:YES triggerDownloadsSync:YES];
}

- (void)removeFileWithIdentifier:(NSString *)fileIdentifier {
    AssertParameterNotNil(fileIdentifier);
    
    // find the existing file
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // if we found the file
    if (file) {
        // remove it
        [self removeFile:file];
    }
}

- (void)removeFile:(LDFile *)file {
    AssertParameterNotNil(file);
    
    [self _removeFile:file triggerListUpdate:YES commitToDisk:YES triggerDownloadsSync:YES];
}

- (void)setFiles:(NSArray *)files {
    AssertParameterIsHomogenousArrayWithElementsOfType(files, LDFile.class);
    
    [self _setFiles:files];
}

- (NSArray *)files {
    return self.filesManifest;
}

#pragma mark - API: Handler blocks

- (void)addBlockForFileUpdatesForFile:(LDFile *)file withBlock:(LDFileUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(file);
    AssertParameterNotNil(block);
    if (!context) context = kDefaultContextName;

    // lazily create a new map table for this file
    if (!self.fileUpdateHandlers[file]) {
        self.fileUpdateHandlers[file] = [NSMapTable new];
    }

    // get the inner map, for easy access
    NSMapTable *fileUpdateHandlersMap = self.fileUpdateHandlers[file];
    
    // lazily create a new array for this context
    if (!fileUpdateHandlersMap[context]) {
        fileUpdateHandlersMap[context] = [NSMutableArray new];
    }
    
    // add the object to the array for this context
    [((NSMutableArray *)fileUpdateHandlersMap[context]) addObject:[block copy]];
}

- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file inContext:(id)context {
    AssertParameterNotNil(file);
    if (!context) context = kDefaultContextName;
    
    // return early if the file doesn't have any updates for it
    if (!self.fileUpdateHandlers[file]) {
        return;
    }
    
    // get the  inner map, for easy access
    NSMapTable *fileUpdateContexts = self.fileUpdateHandlers[file];
    
    // remove the array of handlers for this context
    [fileUpdateContexts removeObjectForKey:context];
    
    // optional clean up, if this was the last context for the file
    if (fileUpdateContexts.count == 1) {
        // remove the map of contexts as well
        [self.fileUpdateHandlers removeObjectForKey:file];
    }
}

- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file {
    AssertParameterNotNil(file);

    // removes the whole map table, including all contexts and all their handlers
    [self.fileUpdateHandlers removeObjectForKey:file];
}

- (void)addBlockForWildcardFileUpdatesWithBlock:(LDFileUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(block);
    if (!context) context = kDefaultContextName;
    
    // lazily create a new array for this context
    if (!self.wildcardFileUpdateHandlers[context]) {
        self.wildcardFileUpdateHandlers[context] = [NSMutableArray new];
    }
    
    // add the object to the array for this context
    [((NSMutableArray *)self.wildcardFileUpdateHandlers[context]) addObject:[block copy]];
}

- (void)removeAllBlocksForFileWildcardUpdatesInContext:(id)context {
    if (!context) context = kDefaultContextName;
    
    // just remove the object (which is an array of handlers) for this context
    [self.wildcardFileUpdateHandlers removeObjectForKey:context];
}

- (void)triggerBlockForFileUpdatesForFile:(LDFile *)file forContext:(id)context {
    [self _sendUpdateForFileWithIdentifier:file.identifier context:context];
}

- (void)addBlockForFileListUpdates:(LDFileListUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(block);
    if (!context) context = kDefaultContextName;
    
    // lazily create a new array for this context
    if (!self.fileListUpdateHandlers[context]) {
        self.fileListUpdateHandlers[context] = [NSMutableArray new];
    }
    
    // add the object to the array for this context
    [((NSMutableArray *)self.fileListUpdateHandlers[context]) addObject:[block copy]];
}

- (void)removeAllBlocksForFileListUpdatesInContext:(id)context {
    if (!context) context = kDefaultContextName;
    
    // just remove the object (which is an array of handlers) for this context
    [self.fileListUpdateHandlers removeObjectForKey:context];
}

- (void)removeAllBlocksForFileListUpdates {
    // just remove all the contexts
    [self.fileListUpdateHandlers removeAllObjects];
}

- (void)triggerBlockForFileListUpdatesForContext:(id)context {
    [self _sendUpdateForFileListForContext:context];
}

#pragma mark - Private: Life

- (id)_init {
    if (self = [super init]) {
        // we create a serial operation queue, so that we can guarantee order of execution
        self.operationQueue = [NSOperationQueue new];
        self.operationQueue.maxConcurrentOperationCount = 1;
        
        // we'll have multiple GBStorages so need to take those into account
        self.GBStorage = GBStorage(kGBStorageNamespace);
        
        // lazy instantiaton of the files manifest
        if (!self.GBStorage[kFilesManifestKey]) {
            self.GBStorage[kFilesManifestKey] = [NSMutableArray new];
            [self _commitFilesListToDisk];
        }
        
        // lazy instantiation of the files status manifest
        if (!self.GBStorage[kFileStatusManifestKey]) {
            self.GBStorage[kFileStatusManifestKey] = [NSMutableDictionary new];
            [self _commitFileStatusManifestToDisk];
        }
        
        // lazy instantiation of the files download progress manifest
        if (!self.GBStorage[kDownloadProgressManifestKey]) {
            self.GBStorage[kDownloadProgressManifestKey] = [NSMutableDictionary new];
            [self _commitDownloadProgressManifestToDisk];
        }
        
        // lazy instantiation of the downloads in progress manifest
        if (!self.GBStorage[kDownloadsInProgressManifestKey]) {
            self.GBStorage[kDownloadsInProgressManifestKey] = [NSMutableSet new];
            [self _commitDownloadsInProgressManifestToDisk];
        }
        
        // local in memory maps
        self.fileListUpdateHandlers = [NSMapTable new];
        self.fileUpdateHandlers = [NSMapTable new];
        self.wildcardFileUpdateHandlers = [NSMapTable new];
        
        // timeout handlers
        self.timeoutHandlers = [NSMutableDictionary new];
        
        // the URL session, of which there can only be one
        self.urlSession = [self _backgroundURLSession];
        
        // add a hook for when internet access comes online, to do a sync
        self.reachability = [Reachability reachabilityForInternetConnection];
        __weak id weakSelf = self;
        self.reachability.reachableBlock = ^(Reachability *reachability) {
            dispatch_async(dispatch_get_main_queue(), ^{
                [weakSelf _triggerDownloadsSync];
            });
        };
        [self.reachability startNotifier];

        // set up a timer to periodically retry failed downloads, as long as we have internet connectivity
        self.automaticRetryingTimer = [NSTimer scheduledTimerWithTimeInterval:kAutomaticRetryingTimeout repeats:YES withBlock:^{
            if (self.reachability.isReachable) {
                [self _triggerDownloadsSync];
            }
        }];
        
        // recreate timeout timers for any still existing tasks
        [self _recreateTimeoutTimersForRunningTasks];
        
        // trigger a downloads sync
        [self _triggerDownloadsSync];
    }
    
    return self;
}

#pragma mark - Private: Task scheduling

- (void)_triggerDownloadsSync {
    // create a copy of the filesManifest, because we can't read from multiple threads, that will cause problems
    NSArray *filesManifest = [self.filesManifest copy];
    
    [self.urlSession getTasksWithCompletionHandler:^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
        // get the current status of the files (on the background thread so that we don't block UI)
        NSMutableDictionary *fileDataStatuses = [NSMutableDictionary new];
        for (LDFile *file in filesManifest) {
            // gather some info relevant to this file
            BOOL hasData = [self _dataExistsOnDiskForFileWithIdentifier:file.identifier];
            fileDataStatuses[file.identifier] = @(hasData);
        }
        
        // -> main thread. we make sure to run this code on the main queue because we need to synchronise some state which is accessed from the main thread
        dispatch_async(dispatch_get_main_queue(), ^{
            // clean up tasks which are no longer needed, these are tasks for which a download is in progress, but no corresponding file exists any more
            for (NSURLSessionTask *task in downloadTasks) {
                // get the file identifier for this download task
                NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
                
                // if file is removed
                if (![self _fileForIdentifier:fileIdentifier]) {
                    // cancel the task permanently
                    [self _cancelDownloadTask:task withDisposition:LDTaskCancellationDispositionPermanent];
                }
            }
            
            // files whose source list has changed, need to have their current downloads permanently cancelled, and their flag for hasOldData set to true (if they have data)
            for (LDFile *file in filesManifest) {
                if (file.hasSourceListChanged) {
                    // find corresponding task
                    NSURLSessionTask *task = [self _taskForFileWithIdentifier:file.identifier fromTasksList:downloadTasks];
                    
                    // permanently cancel the current download if there is one
                    [self _cancelDownloadTask:task withDisposition:LDTaskCancellationDispositionPermanent];
                    
                    // update the flags on the file
                    if ([fileDataStatuses[file.identifier] boolValue]) {
                        file.isDataOutOfDate = YES;
                    }
                    file.hasSourceListChanged = NO;
                }
            }
            
            // start download tasks for files which are not downloaded or are out of date, and for which no download task is already running
            for (LDFile *file in filesManifest) {
                // gather some info relevant to this file
                BOOL hasData = [fileDataStatuses[file.identifier] boolValue];
                BOOL isDataOutOfDate = file.isDataOutOfDate;
                NSURLSessionTask *task = [self _taskForFileWithIdentifier:file.identifier fromTasksList:downloadTasks];
                BOOL hasRunningDownloadTask = (task != nil) || [self _isDownloadInProgressForFileWithIdentifier:file.identifier];
                
                // if the data is there and up to date
                if (hasData && !isDataOutOfDate) {
                    // noop, we don't need to download anything
                }
                // data is not actual and a download task is running
                else if (hasRunningDownloadTask) {
                    // noop, there is already a download task in progress that is servicing this file
                }
                // data isn't there or up to date and no download is servicing this file
                else {
                    // create a new download task for this file
                    [self _createNewDownloadTaskForFile:file];
                }
            }
            
            // we do a batch commit on our manifest because this method might have changed it
            [self _commitFilesListToDisk];
        });
    }];
}

- (void)_cancelDownloadTask:(NSURLSessionTask *)task withDisposition:(LDTaskCancellationDisposition)disposition {
    // attach some meta info to the task so we know in the delegate method how to react to this
    switch (disposition) {
        case LDTaskCancellationDispositionPermanent: {
            [self _attachFlagToTask:task flag:kTaskPayloadFlagCancelledPermanently];
        } break;
            
        case LDTaskCancellationDispositionTimeout: {
            [self _attachFlagToTask:task flag:kTaskPayloadFlagTimedOut];
        } break;
    }
    
    // do the actual cancelling, which triggers the delegate, which triggers our response action
    [task cancel];
}

- (BOOL)_createNewDownloadTaskForFile:(LDFile *)file {
    return [self _createNewDownloadTaskForFile:file withPreviouslyAttemptedSource:nil];
}

- (BOOL)_createNewDownloadTaskForFile:(LDFile *)file withPreviouslyAttemptedSource:(NSURL *)previousSource {
    NSArray *sources = [file _normalizedSources];
    NSUInteger previousSourceIndex = previousSource ? [sources indexOfObject:previousSource] : NSNotFound;

    NSURL *nextSource;

    // if the source isn't found
    if (previousSourceIndex == NSNotFound) {
        // we start from the beginning, as a safety
        nextSource = [sources firstObject];
    }
    // if that was the last source
    else if (previousSourceIndex == (sources.count - 1)) {
        // then we stop trying
        nextSource = nil;
    }
    // otherwise if there are further sources to try
    else {
        // just try the next one
        nextSource = [sources objectAtIndex:(previousSourceIndex + 1)];
    }

    // if we have a source with which to create a download
    if (nextSource) {
        // create and configure the download task
        NSURLSessionDownloadTask *downloadTask = [self.urlSession downloadTaskWithRequest:[NSURLRequest requestWithURL:nextSource]];
        downloadTask.taskDescription = file.identifier;

        // keep track of this download task
        [self _addDownloadToInProgressManifestForFileWithIdentifier:file.identifier];
        
        // start the download task
        [downloadTask resume];
        
        // call our fake delegate method immediately after we start the task
        [self _URLSession:self.urlSession downloadTaskDidStart:downloadTask];
        
        // return YES to indicate that we scheduled a task
        return YES;
    }
    // no more sources to try, that's it we can't do anything now.
    else {
        // return NO to indicate that we didn't schedule a task
        return NO;
    }
}

- (BOOL)_createNewDownloadTaskForFileUsingNextSourceWithOldTask:(NSURLSessionTask *)task {
    // then create a new one to replace it
    return [self _createNewDownloadTaskForFile:[self _fileForDownloadTask:task] withPreviouslyAttemptedSource:task.originalRequest.URL];
}

#pragma mark - Private: Timeout timer

- (void)_recreateTimeoutTimersForRunningTasks {
    // go through all the tasks, the ones which are still properly in progress, and not just the ones which are in the twilight zone (i.e. ones that have just finished downloading but haven't finished writing data yet)
    [self.urlSession getTasksWithCompletionHandler:^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
        [downloadTasks enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
            [self _createTimeoutTimerForDownloadTaskForFileWithIdentifier:[self _fileIdentifierForDownloadTask:obj] withTimeout:kRequestTimeoutAfterInit];
        }];
    }];
}

- (void)_removeAllTimeoutTimers {
    // invalidate all timers
    for (NSString *fileIdentifier in self.timeoutHandlers) {
        NSTimer *timer = self.timeoutHandlers[fileIdentifier];
        [timer invalidate];
    }
    
    // remove all the references
    [self.timeoutHandlers removeAllObjects];
}

- (void)_removeTimeoutTimerForDownloadTaskForFileWithIdentifier:(NSString *)fileIdentifier {
    NSTimer *oldTimer = self.timeoutHandlers[fileIdentifier];
    [oldTimer invalidate];
    [self.timeoutHandlers removeObjectForKey:fileIdentifier];
}

- (void)_createTimeoutTimerForDownloadTaskForFileWithIdentifier:(NSString *)fileIdentifier withTimeout:(NSTimeInterval)timeout {
    // first make sure no old timer exists
    [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier];

    // now create a new timer
    NSTimer *timer = [NSTimer scheduledTimerWithTimeInterval:timeout repeats:NO withBlock:^{
        // find a corresponding download task for this file
        [self.urlSession getTasksWithCompletionHandler:^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
            // try to get the task out
            NSURLSessionTask *task = [self _taskForFileWithIdentifier:fileIdentifier fromTasksList:downloadTasks];
            
            // cancel the task, with a flag that it was cancelled because of a timeout
            if (task) [self _cancelDownloadTask:task withDisposition:LDTaskCancellationDispositionTimeout];
            
            // we should remove the timer from our list to clean up. if the task ends up being rescheduled it will create a new timer, and this will happen after the following line, because it happens through the delegate call
            [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier];
        }];
    }];
    
    // store the timer so we can get to him later
    self.timeoutHandlers[fileIdentifier] = timer;
}

#pragma mark - Private: Download event handlers

- (void)_taskDidStart:(NSURLSessionTask *)task {
    if ([self _fileForDownloadTask:task]) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // update status
        // if we have some data already
        if ([self _dataExistsOnDiskForFileWithIdentifier:fileIdentifier]) {
            // it's downloading, but the old version is still available
            [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusAvailableAndDownloadingNewVersion];
        }
        // no data available
        else {
            // we don't have any old data, just downloading new one
            [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusDownloading];
        }
        
        // set up a timeout timer for our task
        [self _createTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier withTimeout:kRequestTimeoutStandard];
        
        // update file download progress
        [self _setDownloadProgressForFileWithIdentifier:fileIdentifier downloadProgress:0.];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:fileIdentifier];
        
        // we don't add the download task to our manifest here, because this method ends up geting invoked on a subsequent runloop iteration, so instead we have to add the download to the manifest as soon as we created it
    }
}

- (void)_taskDidDownloadSomeData:(NSURLSessionTask *)task totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite {
    if ([self _fileForDownloadTask:task]) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // clear the timeout timer for this task
        [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier];
        
        // update file download progress
        [self _setDownloadProgressForFileWithIdentifier:fileIdentifier withCountOfBytesReceived:totalBytesWritten countOfBytesExpectedToReceive:totalBytesExpectedToWrite];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:fileIdentifier];
    }
}

- (void)_taskWasCancelledPermanently:(NSURLSessionTask *)task {
    // in this handler, we don't check whether the file exists or not because this handler is invoked when tasks are cancelled and this only happens when files are removed. leaving here for clarity of intent
    if (YES) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // clear the timeout timer for this task
        [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier];
        
        // remove the download from the manifest
        [self _removeDownloadFromProgressManifestForFileWithIdentifier:fileIdentifier];
        
        // we're not interested in this task any more, so clear any of this data, because the only time when we permanently cancel a download is when removing the file
        [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier shouldCommit:YES];
        [self _clearStoredFileStatusForFileWithIdentifier:fileIdentifier shouldCommit:YES];
        
        // in this case we don't send a file update, because the associated file no longer exists, unlike in our other handlers
    }
}

- (void)_taskDidFail:(NSURLSessionTask *)task withReason:(LDTaskFailureReason)reason {
    if ([self _fileForDownloadTask:task]) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // clear the timeout timer for this task
        [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:fileIdentifier];
        
        // determine whether we want to reschedule or not
        BOOL shouldTryToReschedule;
        switch (reason) {
            case LDTaskFailureReasonTimeout:
            case LDTaskFailureReasonUnknown: {
                shouldTryToReschedule = YES;
            } break;
                
            case LDTaskFailureReasonFileWritingError: {
                shouldTryToReschedule = NO;
            } break;
        }
        
        // we managed to reschedule the task
        if (shouldTryToReschedule && [self _createNewDownloadTaskForFileUsingNextSourceWithOldTask:task]) {
            // noop. task was rescheduled, now wait for delegate machinery to update us of significant events
        }
        // we didn't reschedule anything, so it's a dead end and the task actually fialed
        else {
            // remove the download from the manifest
            [self _removeDownloadFromProgressManifestForFileWithIdentifier:fileIdentifier];
            
            // update status
            // if we have some data already
            if ([self _dataExistsOnDiskForFileWithIdentifier:fileIdentifier]) {
                // then we just have the old version
                [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusAvailableButOutOfDate];
            }
            // no data available
            else {
                // we don't have anything then
                [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusUnavailable];
            }
        }
        
        // clear file download progress, because the current progress is definitely no longer valid
        [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier];
        
        // send update because the download progress changed, and the status might have changed
        [self _sendUpdateForFileWithIdentifier:fileIdentifier];
    }
}

- (void)_taskDidFinish:(NSURLSessionTask *)task {
    if ([self _fileForDownloadTask:task]) {
        // get the file (as opposed to the identifier, because in this method we need to update the property)
        LDFile *file = [self _fileForDownloadTask:task];
        
        // update the data status
        file.isDataOutOfDate = NO;
        
        // clear the timeout timer for this task
        [self _removeTimeoutTimerForDownloadTaskForFileWithIdentifier:file.identifier];
        
        // remove the download from the manifest
        [self _removeDownloadFromProgressManifestForFileWithIdentifier:file.identifier];
        
        // commit our update to the files list
        [self _commitFilesListToDisk];
        
        // update file status
        [self _setStatusForFileWithIdentifier:file.identifier status:LDFileStatusAvailable];
        
        // clear file download progress
        [self _clearStoredDownloadProgressForFileWithIdentifier:file.identifier];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:file.identifier];
    }
}

#pragma mark - Private: Update handlers

- (void)_sendUpdateForFileWithIdentifier:(NSString *)fileIdentifier {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the contexts for individual file updates
    for (NSString *context in self.fileUpdateHandlers[file]) {
        [self _sendUpdateForFileWithIdentifier:fileIdentifier context:context];
    }
    
    // enumerate the contexts for wildcard file updates
    for (NSString *context in self.wildcardFileUpdateHandlers) {
        [self _sendUpdateForFileWithIdentifierForAllFiles:fileIdentifier context:context];
    }
}

- (void)_sendUpdateForFileWithIdentifier:(NSString *)fileIdentifier context:(id)context {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the handlers in the context
    NSArray *updateHandlers = self.fileUpdateHandlers[file][context ?: kDefaultContextName];
    for (LDFileUpdatedBlock block in updateHandlers) {
        block(file);
    }
}

- (void)_sendUpdateForFileWithIdentifierForAllFiles:(NSString *)fileIdentifier context:(id)context {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the handlers in the context
    NSArray *updateHandlers = self.wildcardFileUpdateHandlers[context ?: kDefaultContextName];
    for (LDFileUpdatedBlock block in updateHandlers) {
        block(file);
    }
}

- (void)_sendUpdateForFileList {
    // enumerate the contexts
    for (NSString *context in self.fileListUpdateHandlers) {
        [self _sendUpdateForFileListForContext:context];
    }
}

- (void)_sendUpdateForFileListForContext:(id)context {
    // enumerate the handlers in the context
    NSArray *updateHandlers = self.fileListUpdateHandlers[context ?: kDefaultContextName];
    for (LDFileListUpdatedBlock block in updateHandlers) {
        block(self.files);
    }
}

#pragma mark - Private: Files list manipulation

- (void)_commitFilesListToDisk {
    [self.GBStorage save:kFilesManifestKey];
}

- (void)_addFile:(LDFile *)file triggerUpdates:(BOOL)shouldTriggerUpdates commitToDisk:(BOOL)shouldCommitToDisk triggerDownloadsSync:(BOOL)shouldTriggerDownloadSync {
    // first check if this file is an exact duplicate of a known file
    if ([self.filesManifest any:^BOOL(id object) {
        return [file isEqualExactly:object];
    }]) {
        // noop, because it doesn't change anything
    }
    // existing file with some changed metadata
    else if ([self.filesManifest containsObject:file]) {
        // find the old file
        LDFile *oldFile = (LDFile *)[self.filesManifest firstObjectEqualToObject:file];

        // prepare the new file
        file.hasSourceListChanged = [oldFile.sources isEqualToArray:file.sources];
        file.isDataOutOfDate = oldFile.isDataOutOfDate;
        
        // update the symlink if the file type has changed
        BOOL hasTypeChanged = ![oldFile.type isEqualToString:file.type];
        if (hasTypeChanged) {
            [self _updateSymlinkForFile:file];
        }
        
        // we update the metadata (by replacing the file)
        [self.filesManifest removeObject:oldFile];
        [self _removeFileFromFilesManifest:oldFile shouldCommit:NO];// we never commit here, because the next line we are chaning it again...
        [self _addFileToFilesManifest:file shouldCommit:shouldCommitToDisk];// and here it makes sense to commit (or not) depending on the caller's wishes
        
        // send update for file, as some metadata might have changed, so we immediately want to let the UI know about this. we don't need to update the list in this branch as we are only updating a file's metadata
        if (shouldTriggerUpdates) [self _sendUpdateForFileWithIdentifier:file.identifier];
    }
    // completely new file
    else {
        // prepare the new file
        file.hasSourceListChanged = NO;
        file.isDataOutOfDate = NO;
        
        // add the file to our list
        [self _addFileToFilesManifest:file shouldCommit:shouldCommitToDisk];
        
        // send updates
        if (shouldTriggerUpdates) {
            // send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
            [self _sendUpdateForFileList];
            
            // then send update for the particular file
            [self _sendUpdateForFileWithIdentifier:file.identifier];
        }
    }
    
    // trigger a sync, even if the file is identical as a fail safe, that is if we've been asked to do so
    if (shouldTriggerDownloadSync) [self _triggerDownloadsSync];
}

- (void)_removeFile:(LDFile *)file triggerListUpdate:(BOOL)shouldTriggerUpdateForFileList commitToDisk:(BOOL)shouldCommitToDisk triggerDownloadsSync:(BOOL)shouldTriggerDownloadSync {
    // remove the handlers
    [self removeAllBlocksForFileUpdatesForFile:file];
    
    // remove the file from the file manifest
    [self _removeFileFromFilesManifest:file shouldCommit:shouldCommitToDisk];
    
    // remove the file from disk, if there is any
    [self _removeStoredDataForFileWithIdentifier:file.identifier];
    
    // remove the symlink as well if there is any
    [self _removeSymlinkForFile:file];
    
    // remove the file status from the status manifest, it might get recreated if there is download task running, but when the download sync is triggered, it will cancel that task, and the handler for that task will then re-clear these
    [self _clearStoredDownloadProgressForFileWithIdentifier:file.identifier shouldCommit:shouldCommitToDisk];
    [self _clearStoredFileStatusForFileWithIdentifier:file.identifier shouldCommit:shouldCommitToDisk];
    
    // send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
    if (shouldTriggerUpdateForFileList) [self _sendUpdateForFileList];
    
    // sync the downloads list
    if (shouldTriggerDownloadSync) [self _triggerDownloadsSync];
}

- (void)_setFiles:(NSArray *)newFiles {
    NSArray *currentFiles = self.filesManifest;
    
    // get list of files which are removed
    NSArray *removals = [currentFiles arrayBySubtractingArray:newFiles];
    
    // files to be removed are removed
    for (LDFile *file in removals) {
        [self _removeFile:file triggerListUpdate:NO commitToDisk:NO triggerDownloadsSync:NO];
    }
    
    // all other files are added using our helper method, which takes care of duplicates and merging
    for (LDFile *file in newFiles) {
        [self _addFile:file triggerUpdates:NO commitToDisk:NO triggerDownloadsSync:NO];
    }
    
    // batch stuff
    // committing to disk
    [self _commitFilesListToDisk];
    [self _commitFileStatusManifestToDisk];
    [self _commitDownloadProgressManifestToDisk];
    
    // list update
    [self _sendUpdateForFileList];// send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
    
    // file updates
    for (LDFile *file in newFiles) {
        [self _sendUpdateForFileWithIdentifier:file.identifier];
    }
    
    // download sync
    [self _triggerDownloadsSync];
}

- (void)_addFileToFilesManifest:(LDFile *)file {
    [self _addFileToFilesManifest:file shouldCommit:YES];
}

- (void)_addFileToFilesManifest:(LDFile *)file shouldCommit:(BOOL)shouldCommit {
    [self.filesManifest addObject:file];
    if (shouldCommit) [self _commitFilesListToDisk];
}

- (void)_removeFileFromFilesManifest:(LDFile *)file {
    [self _removeFileFromFilesManifest:file shouldCommit:YES];
}

- (void)_removeFileFromFilesManifest:(LDFile *)file shouldCommit:(BOOL)shouldCommit {
    [self.filesManifest removeObject:file];
    if (shouldCommit) [self _commitFilesListToDisk];
}

#pragma mark - Private: Symlinks

- (void)_removeSymlinkForFile:(LDFile *)file {
    @synchronized(self) {
        // find a symlink for our file (any file)
        NSString *fileBaseName = [self _baseNameForFileWithIdentifier:file.identifier];
        
        // get the list of symlinks
        NSArray *symlinks = [[NSFileManager defaultManager] contentsOfDirectoryAtURL:[self _diskLocationForSymlinksDirectory] includingPropertiesForKeys:nil options:0 error:nil];
        
        // if any of the matches our file, get rid of it
        for (NSURL *symlink in symlinks) {
            NSString *baseName = [[symlink lastPathComponent] stringByDeletingPathExtension];
            // if we find a matching symlink, get rid of it
            if ([baseName isEqualToString:fileBaseName]) {
                [[NSFileManager defaultManager] removeItemAtURL:symlink error:nil];
            }
            
            // there can only be one symlink
            break;
        }
    }
}

- (BOOL)_createSymlinkForFile:(LDFile *)file {
    @synchronized(self) {
        // make sure the directory exist
        [[NSFileManager defaultManager] createDirectoryAtURL:[self _diskLocationForSymlinksDirectory] withIntermediateDirectories:YES attributes:nil error:nil];
        
        // write the symlink, if we have data
        if ([self _dataExistsOnDiskForFileWithIdentifier:file.identifier]) {
            return [[NSFileManager defaultManager] createSymbolicLinkAtURL:file.dataURLWithExtension withDestinationURL:file.dataURL error:nil];
        }
        else {
            return NO;
        }
    }
}

- (BOOL)_updateSymlinkForFile:(LDFile *)file {
    [self _removeSymlinkForFile:file];
    return [self _createSymlinkForFile:file];
}

- (NSURL *)_diskLocationForSymlinksDirectory {
    return [DocumentsDirectoryURL() URLByAppendingPathComponent:kFileSymlinksDirectory];
}

- (NSURL *)_diskLocationForSymlinkForFile:(LDFile *)file {
    NSURL *fileURL = [[[self _diskLocationForSymlinksDirectory] URLByAppendingPathComponent:[self _baseNameForFileWithIdentifier:file.identifier]] URLByAppendingPathExtension:file.type];
    
    return fileURL;
}

#pragma mark - Private: File storage utilities

- (BOOL)_dataExistsOnDiskForFileWithIdentifier:(NSString *)fileIdentifier {
    @synchronized(self) {
        return [[NSFileManager defaultManager] fileExistsAtPath:[[self _diskLocationForFileWithIdentifier:fileIdentifier] path]];
    }
}

- (NSData *)_dataForFileWithIdentifier:(NSString *)fileIdentifier {
    @synchronized(self) {
        // return data from the disk, or nil if not there
        return [[NSFileManager defaultManager] contentsAtPath:[[self _diskLocationForFileWithIdentifier:fileIdentifier] path]];
    }
}

- (BOOL)_storeDataForFileWithIdentifier:(NSString *)fileIdentifier dataURL:(NSURL *)dataURL {
    @synchronized(self) {
        NSError *error;
        
        // make sure the directory exist
        [[NSFileManager defaultManager] createDirectoryAtURL:[self _diskLocationForDownloadedFilesDirectory] withIntermediateDirectories:YES attributes:nil error:&error];// we don't need to look at the return value here, because if this failed, then the next step will fail too and that will then trigger the error
        
        return [[NSFileManager defaultManager] replaceItemAtURL:[self _diskLocationForFileWithIdentifier:fileIdentifier] withItemAtURL:dataURL backupItemName:nil options:NSFileManagerItemReplacementUsingNewMetadataOnly resultingItemURL:nil error:&error];
    }
}

- (void)_removeStoredDataForFileWithIdentifier:(NSString *)fileIdentifier {
    @synchronized(self) {
        // just remove the file from disk, optimistically, if it doesn't exist, then no biggy
        [[NSFileManager defaultManager] removeItemAtURL:[self _diskLocationForFileWithIdentifier:fileIdentifier] error:nil];
    }
}

- (NSURL *)_diskLocationForDownloadedFilesDirectory {
    return [DocumentsDirectoryURL() URLByAppendingPathComponent:kFileStorageDirectory];
}

- (NSURL *)_diskLocationForFileWithIdentifier:(NSString *)fileIdentifier {
    NSURL *fileURL = [[self _diskLocationForDownloadedFilesDirectory] URLByAppendingPathComponent:[self _baseNameForFileWithIdentifier:fileIdentifier]];
    
    return fileURL;
}

#pragma mark - Private: Misc utilities

- (NSString *)_baseNameForFileWithIdentifier:(NSString *)fileIdentifier {
    return fileIdentifier.md5;
}

- (NSURLSessionTask *)_taskForFileWithIdentifier:(NSString *)fileIdentifier fromTasksList:(NSArray *)tasksList {
    return [tasksList first:^BOOL(id object) {
        return [((NSURLSessionTask *)object).taskDescription isEqualToString:fileIdentifier];
    }];
}

- (LDFile *)_fileForDownloadTask:(NSURLSessionTask *)task {
    return [self _fileForIdentifier:[self _fileIdentifierForDownloadTask:task]];
}

- (NSString *)_fileIdentifierForDownloadTask:(NSURLSessionTask *)task {
    return task.taskDescription;
}

- (LDFile *)_fileForIdentifier:(NSString *)fileIdentifier {
    return [self.filesManifest first:^BOOL(id object) {
        return [((LDFile *)object).identifier isEqualToString:fileIdentifier];
    }];
}

- (NSURLSession *)_backgroundURLSession {
    // Using disptach_once here ensures that multiple background sessions with the same identifier are not created in this instance of the application. If you want to support multiple background sessions within a single process, you should create each session with its own identifier.
    static NSURLSession *session = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        NSURLSessionConfiguration *configuration = [NSURLSessionConfiguration backgroundSessionConfiguration:kURLSessionIdentifier];
        configuration.timeoutIntervalForRequest = kRequestTimeoutStandard;
        configuration.timeoutIntervalForResource = kResourceTotalTimeout;
        session = [NSURLSession sessionWithConfiguration:configuration delegate:self delegateQueue:self.operationQueue];
    });
    return session;
}

- (void)_attachFlagToTask:(NSURLSessionTask *)task flag:(NSString *)flag {
    // synchronized because we might access this from multiple threads
    @synchronized(self) {
        task.GBPayload = flag;
    }
}

- (NSString *)_flagForTask:(NSURLSessionTask *)task {
    // synchronized because we might access this from multiple threads
    @synchronized(self) {
        return (NSString *)task.GBPayload;
    }
}

#pragma mark - Private: Additional properties

- (void)_addDownloadToInProgressManifestForFileWithIdentifier:(NSString *)fileIdentifier {
    [self.downloadsInProgressManifest addObject:fileIdentifier];
    [self _commitDownloadsInProgressManifestToDisk];
}

- (void)_removeDownloadFromProgressManifestForFileWithIdentifier:(NSString *)fileIdentifier {
    [self.downloadsInProgressManifest removeObject:fileIdentifier];
    [self _commitDownloadsInProgressManifestToDisk];
}

- (BOOL)_isDownloadInProgressForFileWithIdentifier:(NSString *)fileIdentifier {
    return [self.downloadsInProgressManifest containsObject:fileIdentifier];
}

- (void)_commitDownloadsInProgressManifestToDisk {
    [self.GBStorage save:kDownloadsInProgressManifestKey];
}

- (void)_setStatusForFileWithIdentifier:(NSString *)fileIdentifier status:(LDFileStatus)status {
    [self _setStatusForFileWithIdentifier:fileIdentifier status:status shouldCommit:YES];
}

- (void)_setStatusForFileWithIdentifier:(NSString *)fileIdentifier status:(LDFileStatus)status shouldCommit:(BOOL)shouldCommit {
    self.fileStatusManifest[fileIdentifier] = @(status);
    if (shouldCommit) [self _commitFileStatusManifestToDisk];
}

- (void)_clearStoredFileStatusForFileWithIdentifier:(NSString *)fileIdentifier {
    [self _clearStoredFileStatusForFileWithIdentifier:fileIdentifier shouldCommit:YES];
}

- (void)_clearStoredFileStatusForFileWithIdentifier:(NSString *)fileIdentifier shouldCommit:(BOOL)shouldCommit {
    [self.fileStatusManifest removeObjectForKey:fileIdentifier];
    if (shouldCommit) [self _commitFileStatusManifestToDisk];
}

- (void)_commitFileStatusManifestToDisk {
    [self.GBStorage save:kFileStatusManifestKey];
}

- (LDFileStatus)_statusForFileWithIdentifier:(NSString *)fileIdentifier {
    // if the status is nil, then it will become zero, which translates to LDFielStatusUnavailabe, which is what we want, but documenting here because it's quite implicit
    return (LDFileStatus)[self.fileStatusManifest[fileIdentifier] unsignedIntegerValue];
}

- (void)_setDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier withCountOfBytesReceived:(int64_t)countOfBytesReceived countOfBytesExpectedToReceive:(int64_t)countOfBytesExpectedToReceive {
    [self _setDownloadProgressForFileWithIdentifier:fileIdentifier withCountOfBytesReceived:countOfBytesReceived countOfBytesExpectedToReceive:countOfBytesExpectedToReceive shouldCommit:YES];
}

- (void)_setDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier withCountOfBytesReceived:(int64_t)countOfBytesReceived countOfBytesExpectedToReceive:(int64_t)countOfBytesExpectedToReceive shouldCommit:(BOOL)shouldCommit {
    // calculate and return the value
    CGFloat downloadProgress;
    if (countOfBytesExpectedToReceive == NSURLSessionTransferSizeUnknown) {
        downloadProgress = kLDDownloadProgressUnknown;
    }
    else {
        downloadProgress = (CGFloat)countOfBytesReceived / (CGFloat)countOfBytesExpectedToReceive;
    }
    
    [self _setDownloadProgressForFileWithIdentifier:fileIdentifier downloadProgress:downloadProgress shouldCommit:shouldCommit];
}

- (void)_setDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier downloadProgress:(CGFloat)downloadProgress {
    [self _setDownloadProgressForFileWithIdentifier:fileIdentifier downloadProgress:downloadProgress shouldCommit:YES];
}

- (void)_setDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier downloadProgress:(CGFloat)downloadProgress shouldCommit:(BOOL)shouldCommit {
    self.downloadProgressManifest[fileIdentifier] = @(downloadProgress);
    if (shouldCommit) [self _commitDownloadProgressManifestToDisk];
}

- (void)_clearStoredDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier {
    [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier shouldCommit:YES];
}

- (void)_clearStoredDownloadProgressForFileWithIdentifier:(NSString *)fileIdentifier shouldCommit:(BOOL)shouldCommit {
    [self.downloadProgressManifest removeObjectForKey:fileIdentifier];
    if (shouldCommit) [self _commitDownloadProgressManifestToDisk];
}

- (void)_commitDownloadProgressManifestToDisk {
    [self.GBStorage save:kDownloadProgressManifestKey];
}

- (CGFloat)_downloadProgressForFileWithIdentifier:(NSString *)fileIdentifier {
    switch ([self _statusForFileWithIdentifier:fileIdentifier]) {
        case LDFileStatusUnavailable: {
            return 0.;
        } break;
            
        case LDFileStatusAvailable:
        case LDFileStatusAvailableButOutOfDate: {
            return 1.;
        } break;
            
        case LDFileStatusDownloading:
        case LDFileStatusAvailableAndDownloadingNewVersion: {
            // if the number coming out of the download manifest is nil, this will return 0, which is what we want. documenting because it's implicit
            return [self.downloadProgressManifest[fileIdentifier] doubleValue];
        } break;
    }
}

#pragma mark - NSURLSessionDelegate

- (void)URLSessionDidFinishEventsForBackgroundURLSession:(NSURLSession *)session {
    // -> main thread
    dispatch_async(dispatch_get_main_queue(), ^{
        // invoke the system completion handler
        if (self.backgroundSessionCompletionHandler) {
            void (^completionHandler)() = self.backgroundSessionCompletionHandler;
            self.backgroundSessionCompletionHandler = nil;
            completionHandler();
        }
    });
}

#pragma mark - NSURLSessionTaskDelegate

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error {
    // -> main thread
    dispatch_async(dispatch_get_main_queue(), ^{
        switch (error.code) {
            case 0: {
                // did the file storage fail?
                if ([[self _flagForTask:task] isEqualToString:kTaskPayloadFlagDataStorageFailed]) {
                    [self _taskDidFail:task withReason:LDTaskFailureReasonFileWritingError];
                }
                // success
                else {
                    [self _taskDidFinish:task];
                }
            } break;
                
            case NSURLErrorCancelled: {
                // was the task scheduled with the intention of being permanently cancelled?
                if ([[self _flagForTask:task] isEqualToString:kTaskPayloadFlagCancelledPermanently]) {
                    [self _taskWasCancelledPermanently:task];
                }
                // the task wasn't cancelled permanently, so treat it as a failure (the task was therefore cancelled with the intention of being replaced)
                else if ([[self _flagForTask:task] isEqualToString:kTaskPayloadFlagTimedOut]) {
                    [self _taskDidFail:task withReason:LDTaskFailureReasonTimeout];
                }
                // this is just a failsafe, shouldn't happen
                else {
                    [self _taskDidFail:task withReason:LDTaskFailureReasonUnknown];
                }
            } break;
                
            default: {
//                NSData *resumeData = [error.userInfo objectForKey:NSURLSessionDownloadTaskResumeData];
                
                [self _taskDidFail:task withReason:LDTaskFailureReasonUnknown];
            } break;
        }
    });
}

#pragma mark - NSURLSessionDownloadDelegate

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didWriteData:(int64_t)bytesWritten totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite {
    // -> main thread
    dispatch_async(dispatch_get_main_queue(), ^{
        [self _taskDidDownloadSomeData:downloadTask totalBytesWritten:totalBytesWritten totalBytesExpectedToWrite:totalBytesExpectedToWrite];
    });
}

// this one isn't actually implemented by the NSURLSessionDownloadDelegate protocol, but I wish it were for consistency sake, so I trigger this one manually
- (void)_URLSession:(NSURLSession *)session downloadTaskDidStart:(NSURLSessionDownloadTask *)downloadTask {
    // -> main thread
    dispatch_async(dispatch_get_main_queue(), ^{
        [self _taskDidStart:downloadTask];
    });
}

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didResumeAtOffset:(int64_t)fileOffset expectedTotalBytes:(int64_t)expectedTotalBytes {
    // -> main thread
    dispatch_async(dispatch_get_main_queue(), ^{
        [self _taskDidStart:downloadTask];
    });
}

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didFinishDownloadingToURL:(NSURL *)downloadURL {
    // get the file
    LDFile *file = [self _fileForIdentifier:[self _fileIdentifierForDownloadTask:downloadTask]];

    // store the file and create a symlink
    BOOL success = [self _storeDataForFileWithIdentifier:file.identifier dataURL:downloadURL];
    success = (success && [self _createSymlinkForFile:file]);
    
    // if the storage failed, mark the task with the correct flag
    if (!success) {
        [self _attachFlagToTask:downloadTask flag:kTaskPayloadFlagDataStorageFailed];
    }
}

@end

#pragma mark - LDFile: Private additions

@implementation LDFile (Private)

// we set dynamic here, because the property is already implemented in the class extension. We need it because that definition is not visible in this file.
@dynamic hasSourceListChanged;
@dynamic isDataOutOfDate;

- (NSArray *)_normalizedSources {
    return [self.sources map:^id(id object) {
        if ([object isKindOfClass:NSURL.class]) {
            return object;
        }
        else if ([object isKindOfClass:NSString.class]) {
            return [NSURL URLWithString:object];
        }
        else {
            @throw [NSException exceptionWithName:NSInternalInconsistencyException reason:[NSString stringWithFormat:@"Did not have a valid URL in the sources. Have object of type %@ with description %@", NSStringFromClass([object class]), [object description]] userInfo:nil];
        }
    }];
}

@end

#pragma mark - LDFile: LiveDepot category

@implementation LDFile (LiveDepot)

- (LDFileStatus)status {
    return [[LiveDepot sharedDepot] _statusForFileWithIdentifier:self.identifier];
}

- (CGFloat)downloadProgress {
    return [[LiveDepot sharedDepot] _downloadProgressForFileWithIdentifier:self.identifier];
}

- (NSData *)data {
    return [[LiveDepot sharedDepot] _dataForFileWithIdentifier:self.identifier];
}

- (NSURL *)dataURL {
    // if the file exists
    if ([[LiveDepot sharedDepot] _dataExistsOnDiskForFileWithIdentifier:self.identifier]) {
        // return the location
        return [[LiveDepot sharedDepot] _diskLocationForFileWithIdentifier:self.identifier];
    }
    // otherwise
    else {
        // return nil
        return nil;
    }
}

- (NSURL *)dataURLWithExtension {
    // if the file exists
    if ([[LiveDepot sharedDepot] _dataExistsOnDiskForFileWithIdentifier:self.identifier]) {
        // return the location
        return [[LiveDepot sharedDepot] _diskLocationForSymlinkForFile:self];
    }
    // otherwise
    else {
        // return nil
        return nil;
    }
}

@end

#pragma mark - NSAray: LiveDepot category

@implementation NSArray (LiveDepot)

- (BOOL)containsFileWithIdentifier:(NSString *)fileIdentifier {
    return [self any:^BOOL(id object) {
        return [((LDFile *)object).identifier isEqualToString:fileIdentifier];
    }];
}

@end



/*
 
General design principles:
 
    - we can just add a file to our list, remove one, switch the whole list up, etc., then trigger a sync of the background downloading tasks
    - when the sync is triggered, we fetch the currently running download tasks, delete ones for which there are no more files, add ones for files for which there is no data and no runnign task already, and for files which have a modified sources list we cancel the old download and start a new one. in each case we update the status of the file to one of (downloading, downloadingAvailable) and same with downloadProgress for these states
    - when removing a file, we just remove it from our list and trigger a sync
    - when a download task receives some data we update the file meta (downloadProgress)
    - when a download task finishes, we save the file and update the meta status to Available
    - when a download fails, we try the next mirror if we have one and leave the state in downloading. if we hava no more mirrors and reachability is available, we set the file status to Unavailable, and schedule the download to retry after some time later, starting from the first mirror again
    - when we loose reachability we cancel all scheduled redownloads
    - when we gain rechability we trigger a sync
    - when the class is init'ed, we trigger a sync
    - when we update metadata, we commit it to disk immediately and efficiently (we should probably use an index, and then a separate file for each file). we commit after an add, remove or set. and before we trigger a sync
    - we send out notifications after updating the file metas, we do both on the main thread
    - when the application is re-awakened, we savae the completionhandler, then we trigger a sync (which happens automatically in the init method as soon as we touch the singleton), and at the end of the sync we send out our update handlers, and then call the completion handler
    - when the application launches on it's own, we reconnect to the session by init'ing the singleton, which we do using a noop startSyncing method
    - we associate download tasks by setting the taskDescription to the identifier of the file
    - the download tasks update the state of our file, and if the user queries the file status, they get a recent snapshot instantly, once the download task is finished we update the file and trigger an update to the client
 
 
Implementation notes:
 
    Test:
        ^succeed (^restart)
        ^cancel (^restart)
        ^multiple files (^restart)
        ^multiple of the same file (^restart)
        fail because network source is unabailable, then bring source back up and see if download continues properly (make the source in question the 3rd mirror or so)
        fail due to some weird reason like bad network, or the other end going dead mid download
        ^fail due to file writing error
        ^ unreachable source (aaksdfsdfds.comm)
        unresponding source/timeout (1.1.1.1)
        network loss
        connection is OK but HTTP status returns 4xx or 5xx
        test when data is being updated, and is out of date, that it performs correctly, try it a bunch of times (I'm worried about threading issues)
        updating a file's sources (with some unreachable sources) while a download is in progress

    Fix:
        ^when starting a download while app is alive, and then crashing app, and leaving it terminated, then when the app is resurrected, the task completes, but a new download for the same file is created again. doesn't happen when the app is reopened in the meantime (bug1)
        ^multiple of the same file problem
        when cycling through sources, sometimes the download progress goes to -100% (bug2) (maybe something to do with the download status being cleared after a failure?, or is our didWriteData task actually returning an unknown file size?)

    Notes:
        register a background task for the app to allow LiveDepot some time to run in the background so that it can honour the timeouts for tasks. We have a timeout of 5 seconds, and are allowed about 10 minutes, so we can work with 120 sources, which is plenty
        ^keep in mind that the file might have been removed from the manifest, by the time some of the delegates fire for a task, so it's a good idea to check if it still exists (e.g. in case of cancellation)
        ^add a lock around the data storage and reading code, because it can be accessed from multiple threads
        ^make sure that I only use the methods which check for a file existence on a background thread, because it can cause the UI to block while the file is being written...although not for very long so it might be ok...this one is infeasible, but we should be ok because files are written very rarely and the duration of the file move is very little
        storing of resume data. are there cases where some resume data can be generated that we would like to reuse?
        do a HEAD request against the server to decide whether a file has really changed, rather than blindly redownloading it (once sources have changed)
        might be cool to have the file size as well (expected if during download, or actual after download)
        would also be cool to be notified of lifecycle events, like file storage failed, task source retried, etc.
 
 */
