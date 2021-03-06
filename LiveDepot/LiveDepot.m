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
static NSString * const kFileStatusManifestKey =                @"FileStatusManifest";
static NSString * const kDownloadProgressManifestKey =          @"DownloadProgressManifest";

static NSString * const kFileStorageDirectory =                 @"com.goonbee.LiveDepot.FilesDirectory.Downloaded";

static NSString * const kTaskPayloadFlagCancelledPermanently =  @"taskCancelledPermanently";
static NSString * const kTaskPayloadFlagTimedOut =              @"taskTimedOut";
static NSString * const kTaskPayloadFlagDataStorageFailed =     @"taskDataStorageFailed";

static NSTimeInterval const kRequestTimeout =                   5;
static NSTimeInterval const kResourceTotalTimeout =             172800;//2 days
static NSTimeInterval const kAutomaticRetryingPeriod =          30;
static NSTimeInterval const kDownloadProgressCommitPeriod =     5;

typedef NS_ENUM(NSUInteger, LDTaskFailureReason) {
    LDTaskFailureReasonUnknown,
    LDTaskFailureReasonFileWritingError,
    LDTaskFailureReasonTimeout,
};

typedef NS_ENUM(NSUInteger, LDTaskCancellationDisposition) {
    LDTaskCancellationDispositionPermanent,
    LDTaskCancellationDispositionTimeout,
};

typedef NS_ENUM(NSUInteger, FileDelta) {
    FileDeltaNoChanges,
    FileDeltaMetadataChanged,
    FileDeltaNewFile,
};

@interface LiveDepot () <NSURLSessionDelegate, NSURLSessionDownloadDelegate>

@property (strong, nonatomic) NSOperationQueue                  *operationQueue;

@property (strong, nonatomic) NSURLSession                      *backgroundURLSession;
@property (strong, nonatomic) NSURLSession                      *resolutionURLSession;

@property (strong, nonatomic) GBStorageController               *GBStorage;

@property (strong, nonatomic) NSMapTable                        *fileListUpdateHandlers;
@property (strong, nonatomic) NSMapTable                        *fileUpdateHandlers;
@property (strong, nonatomic) NSMapTable                        *wildcardFileUpdateHandlers;
@property (strong, nonatomic) NSMutableSet                      *filesMarkedForRepair;
@property (strong, nonatomic) NSMutableSet                      *tasksInResolutionManifest;// this manifest is for which file source has a download task in resolution (and it gets cleared in between sending new resolution requests)
@property (strong, nonatomic) NSMutableSet                      *unresolvedFilesManifest;// this manifest is for which file is still in resolution (and it only gets cleared once the download has been scheduled or failed)

@property (strong, nonatomic, readonly) NSMutableArray          *filesManifest;
@property (strong, nonatomic, readonly) NSMutableDictionary     *fileStatusManifest;
@property (strong, nonatomic, readonly) NSMutableDictionary     *downloadProgressManifest;

@property (strong, nonatomic) NSTimer                           *automaticRetryingTimer;
@property (strong, nonatomic) NSTimer                           *downloadProgressManifestCommitTimer;

@property (strong, nonatomic) Reachability                      *reachability;

@end

@interface LDFile (Private)

@property (assign, nonatomic, readwrite) BOOL                   hasSourceListChanged;
@property (assign, nonatomic, readwrite) BOOL                   isDataOutOfDate;

- (NSArray *)_normalizedSources;

@end

@implementation LiveDepot {
    LDDownloadSchedulingCompletedBLock _didCompleteDownloadSchedulingBlock;
}

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

- (void)setDidCompleteDownloadSchedulingBlock:(LDDownloadSchedulingCompletedBLock)didCompleteDownloadSchedulingBlock {
    // only set it if the block isn't nil
    if (didCompleteDownloadSchedulingBlock) {
        _didCompleteDownloadSchedulingBlock = [didCompleteDownloadSchedulingBlock copy];
    }
}

- (LDDownloadSchedulingCompletedBLock)didCompleteDownloadSchedulingBlock {
    return _didCompleteDownloadSchedulingBlock;
}

#pragma mark - Private: CA

- (void)_clearDidCompleteDownloadSchedulingBlock {
    _didCompleteDownloadSchedulingBlock = nil;
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

- (void)triggerDownloadsSync {
    [self _triggerDownloadsSync];
}

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
    [self setFiles:files willScheduleDownloads:nil];
}

- (void)setFiles:(NSArray *)files willScheduleDownloads:(LDWillScheduleDownloadsBlock)block {
    AssertParameterIsHomogenousArrayWithElementsOfType(files, LDFile.class);
    
    [self _setFiles:files willScheduleDownloads:block];
}

- (NSArray *)files {
    return self.filesManifest;
}

#pragma mark - API: Handler blocks

- (void)addBlockForFileUpdatesForFile:(LDFile *)file withBlock:(LDFileUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(file);
    AssertParameterNotNil(block);
    if (!context) context = [self _defaultContext];
    
    // lazily create a new map table for for this file if needed
    if (!self.fileUpdateHandlers[file]) {
        // {context<weak, identity>:[block]<strong, equality>}
        self.fileUpdateHandlers[file] = [NSMapTable mapTableWithKeyOptions:(NSPointerFunctionsWeakMemory | NSPointerFunctionsObjectPointerPersonality)
                                                              valueOptions:(NSPointerFunctionsStrongMemory | NSPointerFunctionsObjectPersonality)];
    }

    // get the {context:[block]} map
    NSMapTable *fileUpdateHandlersMap = self.fileUpdateHandlers[file];
    
    // lazily create a new array for this context
    if (!fileUpdateHandlersMap[context]) {
        fileUpdateHandlersMap[context] = [NSMutableArray new];
    }
    
    // get the [block] array
    NSMutableArray *fileContextUpdateBlocksArray = fileUpdateHandlersMap[context];
    
    // add the object to the array for this context
    [fileContextUpdateBlocksArray addObject:[block copy]];
}

- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file inContext:(id)context {
    AssertParameterNotNil(file);
    if (!context) context = [self _defaultContext];
    
    // return early if the file doesn't have any updates for it
    if (!self.fileUpdateHandlers[file]) {
        return;
    }
    
    // get the  inner map, for easy access
    NSMapTable *fileUpdateContexts = self.fileUpdateHandlers[file];
    
    // remove the array of handlers for this context
    [fileUpdateContexts removeObjectForKey:context];
    
    // optional clean up, if this was the last context for the file
    if (fileUpdateContexts.count == 0) {
        // remove the map of contexts as well
        [self.fileUpdateHandlers removeObjectForKey:file];
    }
}

- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file {
    AssertParameterNotNil(file);

    // removes the whole map table, including all contexts and all their handlers
    [self.fileUpdateHandlers removeObjectForKey:file];
}

- (void)removeAllBlocksForFileUpdatesInContext:(id)context {
    if (!context) context = [self _defaultContext];

    NSArray *allFilesWithUpdateHandlers = self.fileUpdateHandlers.allKeys;// we get the keys here, rather than just enumerating through it, because the method we call inside the loop mutates the map
    for (LDFile *file in allFilesWithUpdateHandlers) {
        [self removeAllBlocksForFileUpdatesForFile:file inContext:context];
    }
}

- (void)addBlockForWildcardFileUpdatesWithBlock:(LDFileUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(block);
    if (!context) context = [self _defaultContext];
    
    // lazily create a new array for this context
    if (!self.wildcardFileUpdateHandlers[context]) {
        self.wildcardFileUpdateHandlers[context] = [NSMutableArray new];
    }
    
    // add the object to the array for this context
    [((NSMutableArray *)self.wildcardFileUpdateHandlers[context]) addObject:[block copy]];
}

- (void)removeAllBlocksForWildcardFileUpdatesInContext:(id)context {
    if (!context) context = [self _defaultContext];
    
    // just remove the object (which is an array of handlers) for this context
    [self.wildcardFileUpdateHandlers removeObjectForKey:context];
}

- (void)triggerBlockForFileUpdatesForFile:(LDFile *)file forContext:(id)context {
    [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeManualTrigger context:context];
}

- (void)addBlockForFileListUpdates:(LDFileListUpdatedBlock)block inContext:(id)context {
    AssertParameterNotNil(block);
    if (!context) context = [self _defaultContext];
    
    // lazily create a new array for this context
    if (!self.fileListUpdateHandlers[context]) {
        self.fileListUpdateHandlers[context] = [NSMutableArray new];
    }
    
    // add the object to the array for this context
    [((NSMutableArray *)self.fileListUpdateHandlers[context]) addObject:[block copy]];
}

- (void)removeAllBlocksForFileListUpdatesInContext:(id)context {
    if (!context) context = [self _defaultContext];
    
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
        self.operationQueue = [NSOperationQueue mainQueue];
        
        // we'll have multiple GBStorages so need to take those into account
        self.GBStorage = GBStorage(kGBStorageNamespace);
        
        // lazy instantiaton of the files manifest
        if (!self.GBStorage[kFilesManifestKey]) {
            self.GBStorage[kFilesManifestKey] = [NSMutableArray new];
        }
        
        // lazy instantiation of the files status manifest
        if (!self.GBStorage[kFileStatusManifestKey]) {
            self.GBStorage[kFileStatusManifestKey] = [NSMutableDictionary new];
        }
        
        // lazy instantiation of the files download progress manifest
        if (!self.GBStorage[kDownloadProgressManifestKey]) {
            self.GBStorage[kDownloadProgressManifestKey] = [NSMutableDictionary new];
        }
        
        // {context<weak, identity>:[block]<strong, equality>}
        self.fileListUpdateHandlers = [NSMapTable mapTableWithKeyOptions:(NSPointerFunctionsWeakMemory | NSPointerFunctionsObjectPointerPersonality)
                                                            valueOptions:(NSPointerFunctionsStrongMemory | NSPointerFunctionsObjectPersonality)];
        
        // {file<strong, equality>:{context:[block]}<strong, equality>}
        self.fileUpdateHandlers = [NSMapTable mapTableWithKeyOptions:(NSPointerFunctionsStrongMemory | NSPointerFunctionsObjectPersonality)
                                                        valueOptions:(NSPointerFunctionsStrongMemory | NSPointerFunctionsObjectPersonality)];
        
        // {context<weak, identity>:[block]<strong, equality>}
        self.wildcardFileUpdateHandlers = [NSMapTable mapTableWithKeyOptions:(NSPointerFunctionsWeakMemory | NSPointerFunctionsObjectPointerPersonality)
                                                                valueOptions:(NSPointerFunctionsStrongMemory | NSPointerFunctionsObjectPersonality)];
        
        // simple in memory sets
        self.filesMarkedForRepair = [NSMutableSet new];
        self.tasksInResolutionManifest = [NSMutableSet new];
        self.unresolvedFilesManifest = [NSMutableSet new];
        
        // the background URL session, of which there can only be one
        self.backgroundURLSession = [self _makeBackgroundURLSession];
        
        // the resolution URL session
        self.resolutionURLSession = [self _makeResolutionURLSession];
        
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
        self.automaticRetryingTimer = [NSTimer scheduledTimerWithTimeInterval:kAutomaticRetryingPeriod repeats:YES withBlock:^{
            if (self.reachability.isReachable) {
                [self _triggerDownloadsSync];
            }
        }];
        
        // set up a timer to periodically commit the download progress to disk
        self.downloadProgressManifestCommitTimer = [NSTimer scheduledTimerWithTimeInterval:kDownloadProgressCommitPeriod repeats:YES withBlock:^{
            [self _commitDownloadProgressManifestToDisk];
        }];
        
        // trigger a downloads sync
        [self _triggerDownloadsSync];
    }
    
    return self;
}

#pragma mark - Private: Task scheduling

- (void)_triggerDownloadsSync {
    [self _triggerDownloadsSyncWillScheduleDownloads:nil];
}

- (void)_triggerDownloadsSyncWillScheduleDownloads:(LDWillScheduleDownloadsBlock)block {
    // get the current snapshot of tasks
    [self.backgroundURLSession getTasksWithCompletionHandler:^(NSArray *dataTasks, NSArray *uploadTasks, NSArray *downloadTasks) {
        // we create a copy in case the code below ends up mutating the filesManifest
        NSArray *filesManifest = [self.filesManifest copy];
        
        // clean up tasks which are no longer needed, these are tasks for which a download is in progress, but no corresponding file exists any more
        NSArray *tasksWithoutFiles = [downloadTasks filter:^BOOL(id object) {
            // get the file identifier for this download task
            NSString *fileIdentifierForTask = [self _fileIdentifierForDownloadTask:object];
            
            // check if this task is without a corresponding file
            BOOL withoutFile = ![self _fileForIdentifier:fileIdentifierForTask];
            return withoutFile;
        }];
        [tasksWithoutFiles enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
            [self _cancelDownloadTask:obj withDisposition:LDTaskCancellationDispositionPermanent];
        }];
        
        // files which are in a transient state that indicate that more delegate calls are expected which will transition it into a stable state, but don't have a download running, need to be reset into a stable state
        NSArray *relevantTasks = [downloadTasks arrayBySubtractingArray:tasksWithoutFiles];
        for (NSURLSessionTask *task in relevantTasks) {
            // get the corresponding task
            NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];

            // make sure we have a valid fileIdentifier
            if (fileIdentifier) {
                // if there is no task
                if (!task) {
                    // then we can mark this file for repair
                    [self _markFileWithIdentifierForStatusRepair:fileIdentifier];
                }
                // there is a task, we have to check what state it's in to see if any more delegate messages might be sent
                else {
                    // we have to make sure it's in a final state
                    switch (task.state) {
                        case NSURLSessionTaskStateCompleted: {
                            // the task will send no further delegate messages, so we can mark it for repair
                            [self _markFileWithIdentifierForStatusRepair:fileIdentifier];
                        } break;
                            
                        case NSURLSessionTaskStateRunning:
                        case NSURLSessionTaskStateCanceling:
                        case NSURLSessionTaskStateSuspended: {
                            // the task might send further delegate messages, so wait until it completes (which includes failures)
                        } break;
                    }
                }
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
                if ([self _dataExistsOnDiskForFile:file]) {
                    file.isDataOutOfDate = YES;
                }
                file.hasSourceListChanged = NO;
            }
        }
        
        // start download tasks for files which are not downloaded or are out of date, and for which no download task is already running. keep track of how many we schedule
        NSUInteger toBeScheduledCount = 0;
        for (LDFile *file in filesManifest) {
            // gather some info relevant to this file
            BOOL hasData = [self _dataExistsOnDiskForFile:file];
            BOOL isDataOutOfDate = file.isDataOutOfDate;
            NSURLSessionTask *task = [self _taskForFileWithIdentifier:file.identifier fromTasksList:downloadTasks];
            BOOL hasRunningDownloadTask = (task != nil);
            
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
                toBeScheduledCount += 1;
            }
        }
        
        // we do a batch commit on our manifest because this method might have changed it
        [self _commitFilesListToDisk];
        
        // if we had a block, let them know how many we scheduled
        if (block) block(toBeScheduledCount);
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

// used by the sync trigger because he doesn't need to know whether the task was actually created or not
- (void)_createNewDownloadTaskForFile:(LDFile *)file {
    // mark the file as unresolved
    [self _markFileWithIdentifierAsUnresolved:file.identifier];
    
    // create the download
    [self __createDownloadTaskForFile:file withPreviouslyAttemptedSource:nil willCreate:nil];
}

// used by the failure handler, he needs to know whethe rthis was the tasks last source in which case he can declare a failure, or whether the task was replaced in which case he stays quiet and defers to his future self who will get the outcome via the delegate callback of the newly created replacement task
- (void)_createNewReplacementDownloadTaskForFileUsingNextSourceWithOldTask:(NSURLSessionTask *)task created:(VoidBlockBool)block {
    // mark the file as unresolved
    [self _markFileWithIdentifierAsUnresolved:[self _fileIdentifierForDownloadTask:task]];
    
    // then create a new one to replace it
    [self __createDownloadTaskForFile:[self _fileForDownloadTask:task] withPreviouslyAttemptedSource:task.originalRequest.URL willCreate:block];
}

- (void)__createDownloadTaskForFile:(LDFile *)file withPreviouslyAttemptedSource:(NSURL *)previousSource willCreate:(VoidBlockBool)block {
    // if we're already resolving a task for this file, then we shouldn't do anything here, and stay idempotent
    if ([self _isResolvingTaskForFileWithIdentifier:file.identifier]) {
        if (block) block(NO);
    }
    // this task isn't being resolved, so go ahead
    else {
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
            // resolve the source, before we start the download
            [self _resolveSource:nextSource forFileWithIdentifier:file.identifier withCompletionHandler:^(BOOL sourceIsGood) {
                // the source was bad
                if (!sourceIsGood) {
                    // recurse, trying with the next source
                    [self __createDownloadTaskForFile:file withPreviouslyAttemptedSource:nextSource willCreate:block];
                }
                // the source was good, just create a normal download
                else {
                    // return YES to indicate that we managed to find another valid source and will schedule the replacement task
                    if (block) block(YES);
                    
                    // create and configure the download task
                    NSURLSessionDownloadTask *downloadTask = [self.backgroundURLSession downloadTaskWithRequest:[NSURLRequest requestWithURL:nextSource]];
                    downloadTask.taskDescription = file.identifier;
                    
                    // start the download task
                    [downloadTask resume];
                    
                    // call our fake delegate method immediately after we actually started the task
                    [self _URLSession:self.backgroundURLSession downloadTaskDidStart:downloadTask];
                }
            }];
        }
        // no more sources to try, that's it we can't do anything now.
        else {
            // return NO to indicate that we didn't schedule a task
            if (block) block(NO);
        }
    }
}

#pragma mark - Private: Task resolution

- (void)_resolveSource:(NSURL *)source forFileWithIdentifier:(NSString *)fileIdentifier withCompletionHandler:(VoidBlockBool)block {
    AssertParameterNotNil(block);
    
    [self _addTaskForFileWithIdentifierToResolutionsList:fileIdentifier];
    
    // prep request
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:source];
    [request setHTTPMethod:@"HEAD"];
    
    // try to resolve the source
    [[self.resolutionURLSession dataTaskWithRequest:request completionHandler:^(NSData *data, NSURLResponse *response, NSError *error) {
        // we're no longer resolving
        [self _removeTaskForFileWithIdentifierFromResolutionsList:fileIdentifier];
        
        // there is a response
        if ([response isKindOfClass:NSHTTPURLResponse.class]) {
            NSHTTPURLResponse *httpResponse = (NSHTTPURLResponse *)response;
            
            // ok: 2xx, 3xx
            if (httpResponse.statusCode >= 200 && httpResponse.statusCode < 400) {
                block(YES);
            }
            // not ok
            else {
                block(NO);
            }
        }
        // no response
        else {
            block(NO);
        }
    }] resume];
}

- (BOOL)_isResolvingTaskForFileWithIdentifier:(NSString *)fileIdentifier {
    return [self.tasksInResolutionManifest containsObject:fileIdentifier];
}

- (void)_addTaskForFileWithIdentifierToResolutionsList:(NSString *)fileIdentifier {
    [self.tasksInResolutionManifest addObject:fileIdentifier];
}

- (void)_removeTaskForFileWithIdentifierFromResolutionsList:(NSString *)fileIdentifier {
    [self.tasksInResolutionManifest removeObject:fileIdentifier];
}

- (void)_markFileWithIdentifierAsUnresolved:(NSString *)fileIdentifier {
    [self.unresolvedFilesManifest addObject:fileIdentifier];
}

- (void)_markFileWithIdentifierAsResolved:(NSString *)fileIdentifier {
    NSUInteger previousCount = self.unresolvedFilesManifest.count;
    [self.unresolvedFilesManifest removeObject:fileIdentifier];
    NSUInteger newCount = self.unresolvedFilesManifest.count;
    
    // if this was the last file in the resolution, we can let the client know that this was the last block to be scheduled
    if ((previousCount != newCount) && (newCount == 0)) {
        if (self.didCompleteDownloadSchedulingBlock) self.didCompleteDownloadSchedulingBlock();
        [self _clearDidCompleteDownloadSchedulingBlock];
    }
}

#pragma mark - Private: File repair

- (void)_markFileWithIdentifierForStatusRepair:(NSString *)fileIdentifier {
    // check if this file has already been marked for repair, if so do the repair. we use a 2 step process, a file has to be marked for repair twice, before it's removed
    if ([self.filesMarkedForRepair containsObject:fileIdentifier]) {
        // do the actual repair
        [self _repairFileWithIdentifier:fileIdentifier];
    }
    // the file hasn't been marked
    else {
        // so for now just mark it
        [self.filesMarkedForRepair addObject:fileIdentifier];
    }
}

- (void)_markFileWithIdentifierAsHavingUpToDateStatus:(NSString *)fileIdentifier {
    [self.filesMarkedForRepair removeObject:fileIdentifier];
}

- (void)_repairFileWithIdentifier:(NSString *)fileIdentifier {
    // get the file
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // if the file is still in existence, update it's status
    if (file) {
        // check if there is data for this file on disk
        BOOL dataExists = [self _dataExistsOnDiskForFile:file];
        BOOL dataMarkedAsOutOfDate = file.isDataOutOfDate;
        
        // up to date
        if (dataExists && !dataMarkedAsOutOfDate) {
            [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusAvailable];
        }
        // data there but out of date
        else if (dataExists && dataMarkedAsOutOfDate) {
            [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusAvailableButOutOfDate];
        }
        // no data
        else {
            [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusUnavailable];
        }
    }
    
    // clear file download progress
    [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier];
    
    // the status is now up to date, so remove it from the marked-for-repair manifest
    [self _markFileWithIdentifierAsHavingUpToDateStatus:fileIdentifier];
    
    // send an update
    [self _sendUpdateForFileWithIdentifier:fileIdentifier type:LDFileUpdateTypeRepairEvent];
}

#pragma mark - Private: Download event handlers

- (void)_taskDidStart:(NSURLSessionTask *)task {
    // mark the file as resolved
    if ([self _fileIdentifierForDownloadTask:task]) [self _markFileWithIdentifierAsResolved:[self _fileIdentifierForDownloadTask:task]];
    
    LDFile *file = [self _fileForDownloadTask:task];
    if (file) {
        // update status
        // if we have some data already
        if ([self _dataExistsOnDiskForFile:file]) {
            // it's downloading, but the old version is still available
            [self _setStatusForFileWithIdentifier:file.identifier status:LDFileStatusAvailableAndDownloadingNewVersion];
        }
        // no data available
        else {
            // we don't have any old data, just downloading new one
            [self _setStatusForFileWithIdentifier:file.identifier status:LDFileStatusDownloading];
        }
        
        // the status is up to date
        [self _markFileWithIdentifierAsHavingUpToDateStatus:file.identifier];
        
        // update file download progress
        [self _setDownloadProgressForFileWithIdentifier:file.identifier downloadProgress:0. shouldCommit:NO];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeDownloadStarted];
        
        // we don't add the download task to our manifest here, because this method ends up geting invoked on a subsequent runloop iteration, so instead we have to add the download to the manifest as soon as we created it
    }
}

- (void)_taskDidDownloadSomeData:(NSURLSessionTask *)task totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite {
    LDFile *file = [self _fileForDownloadTask:task];
    if (file) {
        // update file download progress
        [self _setDownloadProgressForFileWithIdentifier:file.identifier withCountOfBytesReceived:totalBytesWritten countOfBytesExpectedToReceive:totalBytesExpectedToWrite shouldCommit:NO];
        
        // the status is up to date
        [self _markFileWithIdentifierAsHavingUpToDateStatus:file.identifier];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeDownloadProgressChanged];
    }
}

- (void)_taskWasCancelledPermanently:(NSURLSessionTask *)task {
    // mark the file as resolved
    if ([self _fileIdentifierForDownloadTask:task]) [self _markFileWithIdentifierAsResolved:[self _fileIdentifierForDownloadTask:task]];
    
    // in this handler, we don't check whether the file exists or not because this handler is invoked when tasks are cancelled and this only happens when files are removed. leaving here for clarity of intent
    if (YES) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // we need to ensure that there is a valid identifier attached to the task though
        if (fileIdentifier) {
            // the status is up to date
            [self _markFileWithIdentifierAsHavingUpToDateStatus:fileIdentifier];
            
            // we're not interested in this task any more, so clear any of this data, because the only time when we permanently cancel a download is when removing the file
            [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier shouldCommit:YES];
            [self _clearStoredFileStatusForFileWithIdentifier:fileIdentifier shouldCommit:YES];
            
            // in this case we don't send a file update, because the associated file no longer exists, unlike in our other handlers
        }
    }
}

- (void)_taskDidFail:(NSURLSessionTask *)task withReason:(LDTaskFailureReason)reason {
    // mark the file as resolved
    if ([self _fileIdentifierForDownloadTask:task]) [self _markFileWithIdentifierAsResolved:[self _fileIdentifierForDownloadTask:task]];
    
    LDFile *file = [self _fileForDownloadTask:task];
    if (file) {
        NSString *fileIdentifier = [self _fileIdentifierForDownloadTask:task];
        
        // this is what we need to do once we have definitively failed, with no chance of resurrection
        VoidBlock cleanupBlock = ^{
            // update status
            // if we have some data already
            if ([self _dataExistsOnDiskForFile:file]) {
                // then we just have the old version
                [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusAvailableButOutOfDate];
            }
            // no data available
            else {
                // we don't have anything then
                [self _setStatusForFileWithIdentifier:fileIdentifier status:LDFileStatusUnavailable];
            }
            
            // clear file download progress, because the current progress is definitely no longer valid
            [self _clearStoredDownloadProgressForFileWithIdentifier:fileIdentifier];
            
            // the status is up to date
            [self _markFileWithIdentifierAsHavingUpToDateStatus:fileIdentifier];
            
            // send update because the status has changed
            [self _sendUpdateForFileWithIdentifier:fileIdentifier type:LDFileUpdateTypeDownloadFailed];
        };
        
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
        
        // we shouldn't reschedule anything, so we know immediately that it's a dead end and the task actually failed
        if (!shouldTryToReschedule) {
            cleanupBlock();
            return;
        }
        // we should try to reschedule it, so we can't just yet determine whether it was a failed attempt or not
        else {
            // try to replace the task
            [self _createNewReplacementDownloadTaskForFileUsingNextSourceWithOldTask:task created:^(BOOL willCreate) {
                // we will replace the task
                if (willCreate) {
                    // noop. we will create a new download task, and its outcome will then find its way back to us via the delegates
                }
                else {
                    // the task won't get replaced, so this is a dead end now
                    cleanupBlock();
                }
            }];
        }
    }
}

- (void)_taskDidFinish:(NSURLSessionTask *)task {
    // mark the file as resolved
    if ([self _fileIdentifierForDownloadTask:task]) [self _markFileWithIdentifierAsResolved:[self _fileIdentifierForDownloadTask:task]];
    
    LDFile *file = [self _fileForDownloadTask:task];
    if (file) {
        // update the data status
        file.isDataOutOfDate = NO;
        
        // clear file download progress, because the current progress is definitely no longer valid
        [self _clearStoredDownloadProgressForFileWithIdentifier:file.identifier];
        
        // commit our update to the files list
        [self _commitFilesListToDisk];
        
        // update file status
        [self _setStatusForFileWithIdentifier:file.identifier status:LDFileStatusAvailable];
        
        // send update
        [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeDownloadSucceeded];
    }
}

#pragma mark - Private: Update handlers

- (void)_sendUpdateForFileWithIdentifier:(NSString *)fileIdentifier type:(LDFileUpdateType)updateType {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the contexts for individual file updates
    for (NSString *context in self.fileUpdateHandlers[file]) {
        [self _sendUpdateForFileWithIdentifier:fileIdentifier type:updateType context:context];
    }
    
    // enumerate the contexts for wildcard file updates
    for (NSString *context in self.wildcardFileUpdateHandlers) {
        [self _sendUpdateForFileWithIdentifierForAllFiles:fileIdentifier type:updateType context:context];
    }
}

- (void)_sendUpdateForFileWithIdentifier:(NSString *)fileIdentifier type:(LDFileUpdateType)updateType context:(id)context {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the handlers in the context
    NSArray *updateHandlers = self.fileUpdateHandlers[file][context ?: [self _defaultContext]];
    for (LDFileUpdatedBlock block in updateHandlers) {
        block(file, updateType);
    }
}

- (void)_sendUpdateForFileWithIdentifierForAllFiles:(NSString *)fileIdentifier type:(LDFileUpdateType)updateType context:(id)context {
    LDFile *file = [self _fileForIdentifier:fileIdentifier];
    
    // enumerate the handlers in the context
    NSArray *updateHandlers = self.wildcardFileUpdateHandlers[context ?: [self _defaultContext]];
    for (LDFileUpdatedBlock block in updateHandlers) {
        block(file, updateType);
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
    NSArray *updateHandlers = self.fileListUpdateHandlers[context ?: [self _defaultContext]];
    for (LDFileListUpdatedBlock block in updateHandlers) {
        block(self.files);
    }
}

#pragma mark - Private: Files list manipulation

- (FileDelta)_deltaForFile:(LDFile *)file {
    if ([self.filesManifest any:^BOOL(id object) {
        return [file isEqualExactly:object];
    }]) {
        return FileDeltaNoChanges;
    }
    // existing file with some changed metadata
    else if ([self.filesManifest containsObject:file]) {
        return FileDeltaMetadataChanged;
    }
    // completely new file
    else {
        return FileDeltaNewFile;
    }
}

- (void)_commitFilesListToDisk {
    [self.GBStorage save:kFilesManifestKey];
}

- (void)_addFile:(LDFile *)file triggerUpdates:(BOOL)shouldTriggerUpdates commitToDisk:(BOOL)shouldCommitToDisk triggerDownloadsSync:(BOOL)shouldTriggerDownloadSync {
    // create a copy of the new file
    LDFile *newFile = [file copy];
    
    switch ([self _deltaForFile:newFile]) {
        case FileDeltaNoChanges: {
            // noop, because it doesn't change anything
        } break;
            
        case FileDeltaMetadataChanged: {
            // find the old file
            LDFile *oldFile = (LDFile *)[self.filesManifest firstObjectEqualToObject:newFile];
            
            // prepare the new file
            newFile.hasSourceListChanged = ![oldFile.sources isEqualToArray:newFile.sources];
            newFile.isDataOutOfDate = oldFile.isDataOutOfDate;
            
            // update the symlink if the file type has changed
            BOOL hasTypeChanged = ![oldFile.type isEqualToString:newFile.type];
            if (hasTypeChanged) {
                [self _updateFileLocationForFile:newFile];
            }
            
            // we update the metadata (by replacing the file)
            [self.filesManifest removeObject:oldFile];
            [self _removeFileFromFilesManifest:oldFile shouldCommit:NO];// we never commit here, because the next line we are chaning it again...
            [self _addFileToFilesManifest:newFile shouldCommit:shouldCommitToDisk];// and here it makes sense to commit (or not) depending on the caller's wishes
            
            // send update for file, as some metadata might have changed, so we immediately want to let the UI know about this. we don't need to update the list in this branch as we are only updating a file's metadata
            if (shouldTriggerUpdates) [self _sendUpdateForFileWithIdentifier:newFile.identifier type:LDFileUpdateTypeMetadataChanged];
        } break;
            
        case FileDeltaNewFile: {
            // prepare the new file
            newFile.hasSourceListChanged = NO;
            newFile.isDataOutOfDate = NO;
            
            // add the file to our list
            [self _addFileToFilesManifest:newFile shouldCommit:shouldCommitToDisk];
            
            // send updates
            if (shouldTriggerUpdates) {
                // send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
                [self _sendUpdateForFileList];
                
                // then send update for the particular file
                [self _sendUpdateForFileWithIdentifier:newFile.identifier type:LDFileUpdateTypeNewFileAdded];
            }
        } break;
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
    [self _removeStoredDataForFile:file];
    
    // remove the file status from the status manifest, it might get recreated if there is download task running, but when the download sync is triggered, it will cancel that task, and the handler for that task will then re-clear these
    [self _clearStoredDownloadProgressForFileWithIdentifier:file.identifier shouldCommit:shouldCommitToDisk];
    [self _clearStoredFileStatusForFileWithIdentifier:file.identifier shouldCommit:shouldCommitToDisk];
    
    // send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
    if (shouldTriggerUpdateForFileList) [self _sendUpdateForFileList];
    
    // sync the downloads list
    if (shouldTriggerDownloadSync) [self _triggerDownloadsSync];
}

- (void)_setFiles:(NSArray *)newFiles willScheduleDownloads:(LDWillScheduleDownloadsBlock)block {
    NSArray *currentFiles = self.filesManifest;
    
    // get list of files which are removed
    NSArray *removals = [currentFiles arrayBySubtractingArray:newFiles];
    
    // files to be removed are removed
    for (LDFile *file in removals) {
        [self _removeFile:file triggerListUpdate:NO commitToDisk:NO triggerDownloadsSync:NO];
    }
    
    // get the deltas for the files (before they are committed into our list), we need this so we can tell the client what kind of updates we made.
    NSMutableDictionary *fileDeltas = [NSMutableDictionary new];
    for (LDFile *file in newFiles) {
        fileDeltas[file] = @([self _deltaForFile:file]);
    }
    
    // all other files are added using our helper method, which takes care of duplicates and merging
    for (LDFile *file in newFiles) {
        [self _addFile:file triggerUpdates:NO commitToDisk:NO triggerDownloadsSync:NO];
    }
    
    // batch stuff
    // committing to disk
    [self _commitFilesListToDisk];
    [self _commitFileStatusManifestToDisk];
    
    // list update
    [self _sendUpdateForFileList];// send update for file list, first because it is often as a result of updating the file list that we register handlers for file updates
    
    // file updates
    for (LDFile *file in fileDeltas) {
        switch ((FileDelta)[fileDeltas[file] unsignedIntegerValue]) {
            case FileDeltaNewFile: {
                [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeNewFileAdded];
            } break;

            case FileDeltaMetadataChanged: {
                [self _sendUpdateForFileWithIdentifier:file.identifier type:LDFileUpdateTypeMetadataChanged];
            } break;
                
            case FileDeltaNoChanges: {
                // noop, the file didn't change so we don't need to send an update for it
            } break;
        }
    }
    
    // download sync
    [self _triggerDownloadsSyncWillScheduleDownloads:block];
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

#pragma mark - Private: File storage utilities

- (BOOL)_dataExistsOnDiskForFile:(LDFile *)file {
    return [[NSFileManager defaultManager] fileExistsAtPath:[[self _diskLocationForFile:file] path]];
}

- (NSData *)_dataForFile:(LDFile *)file {
    // return data from the disk, or nil if not there
    return [[NSFileManager defaultManager] contentsAtPath:[[self _diskLocationForFile:file] path]];
}

- (BOOL)_storeDataForFile:(LDFile *)file dataURL:(NSURL *)dataURL {
    NSError *error;
    
    // make sure the directory exist
    [[NSFileManager defaultManager] createDirectoryAtURL:[self _diskLocationForDownloadedFilesDirectory] withIntermediateDirectories:YES attributes:nil error:&error];// we don't need to look at the return value here, because if this failed, then the next step will fail too and that will then trigger the error
    
    return [[NSFileManager defaultManager] replaceItemAtURL:[self _diskLocationForFile:file] withItemAtURL:dataURL backupItemName:nil options:NSFileManagerItemReplacementUsingNewMetadataOnly resultingItemURL:nil error:&error];
}

- (void)_removeStoredDataForFile:(LDFile *)file {
    // just remove the file from disk, optimistically, if it doesn't exist, then no biggy
    [[NSFileManager defaultManager] removeItemAtURL:[self _diskLocationForFile:file] error:nil];
}

- (void)_updateFileLocationForFile:(LDFile *)file {
    NSError *error;
    
    // find a file for our file (any file)
    NSString *fileBaseName = [self _baseNameForFileWithIdentifier:file.identifier];
    
    // get the list of symlinks
    NSArray *paths = [[NSFileManager defaultManager] contentsOfDirectoryAtURL:[self _diskLocationForDownloadedFilesDirectory] includingPropertiesForKeys:nil options:0 error:&error];
    
    // enumerate the stored files
    for (NSURL *currectLocation in paths) {
        NSString *baseName = [[currectLocation lastPathComponent] stringByDeletingPathExtension];
        NSString *extension = [[currectLocation lastPathComponent] pathExtension];
        
        BOOL baseNameMatches = [baseName isEqualToString:fileBaseName];
        BOOL extensionMatches = [extension isEqualToString:file.type];
        
        // if we find a file that matches exactly
        if (baseNameMatches && extensionMatches) {
            // our job is done, the file is already in the correct location
            break;
        }
        // if only the base name matches
        else if (baseNameMatches && !extensionMatches) {
            // we need to move the file into the right place
            
            [[NSFileManager defaultManager] replaceItemAtURL:[self _diskLocationForFile:file] withItemAtURL:currectLocation backupItemName:nil options:NSFileManagerItemReplacementUsingNewMetadataOnly resultingItemURL:nil error:&error];
            
            // job done
            break;
        }
        else {
            // noop, keep searching
        }
    }
}

- (NSURL *)_diskLocationForDownloadedFilesDirectory {
    return [DocumentsDirectoryURL() URLByAppendingPathComponent:kFileStorageDirectory];
}

- (NSURL *)_diskLocationForFile:(LDFile *)file {
    NSURL *fileURL = [[[self _diskLocationForDownloadedFilesDirectory] URLByAppendingPathComponent:[self _baseNameForFileWithIdentifier:file.identifier]] URLByAppendingPathExtension:file.type];;
    
    return fileURL;
}

#pragma mark - Private: Misc utilities

- (id)_defaultContext {
    // we need some object which we use as the default context, to replace nil as nil can't be inserted into maps. We always need the same object. We could create some object and always use that one, but we already have an object lying around that won't change and will live for the lifetime of this instance... self
    return self;
}

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

- (NSURLSession *)_makeResolutionURLSession {
    NSURLSessionConfiguration *configuration = [NSURLSessionConfiguration defaultSessionConfiguration];
    configuration.timeoutIntervalForRequest = kRequestTimeout;
    configuration.timeoutIntervalForResource = kRequestTimeout;// we use the request timeout, not the total resource timeout, because this URLSession is just for the HEAD requests
    
    // create a new URLSession
    return [NSURLSession sessionWithConfiguration:configuration delegate:nil delegateQueue:self.operationQueue];
}

- (NSURLSession *)_makeBackgroundURLSession {
    NSURLSessionConfiguration *configuration;
    // iOS 8+
    if ([NSURLSessionConfiguration.class respondsToSelector:@selector(backgroundSessionConfigurationWithIdentifier:)]) {
        configuration = [NSURLSessionConfiguration backgroundSessionConfigurationWithIdentifier:kURLSessionIdentifier];
    }
    // iOS 7
    else {
        configuration = [NSURLSessionConfiguration backgroundSessionConfiguration:kURLSessionIdentifier];
    }
    configuration.timeoutIntervalForRequest = kRequestTimeout;
    configuration.timeoutIntervalForResource = kResourceTotalTimeout;
    
    return [NSURLSession sessionWithConfiguration:configuration delegate:self delegateQueue:self.operationQueue];
}

- (void)_attachFlagToTask:(NSURLSessionTask *)task flag:(NSString *)flag {
    task.GBPayload = flag;
}

- (NSString *)_flagForTask:(NSURLSessionTask *)task {
    return (NSString *)task.GBPayload;
}

#pragma mark - Private: Additional properties

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
    // if the status is nil, then it will become zero, which translates to LDFileStatusUnavailabe, which is what we want, but documenting here because it's quite implicit
    return (LDFileStatus)[self.fileStatusManifest[fileIdentifier] unsignedIntegerValue];
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
    // invoke the system completion handler
    if (self.backgroundSessionCompletionHandler) {
        void (^completionHandler)() = self.backgroundSessionCompletionHandler;
        self.backgroundSessionCompletionHandler = nil;
        completionHandler();
    }
}

- (void)URLSession:(NSURLSession *)session didBecomeInvalidWithError:(NSError *)error {
    if (session == self.backgroundURLSession) {
        self.backgroundURLSession = [self _makeBackgroundURLSession];
    }
    else if (session == self.resolutionURLSession) {
        self.resolutionURLSession = [self _makeResolutionURLSession];
    }
}

#pragma mark - NSURLSessionTaskDelegate

- (void)URLSession:(NSURLSession *)session task:(NSURLSessionTask *)task didCompleteWithError:(NSError *)error {
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
}

#pragma mark - NSURLSessionDownloadDelegate

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didWriteData:(int64_t)bytesWritten totalBytesWritten:(int64_t)totalBytesWritten totalBytesExpectedToWrite:(int64_t)totalBytesExpectedToWrite {
    [self _taskDidDownloadSomeData:downloadTask totalBytesWritten:totalBytesWritten totalBytesExpectedToWrite:totalBytesExpectedToWrite];
}

// this one isn't actually implemented by the NSURLSessionDownloadDelegate protocol, but I wish it were for consistency sake, so I trigger this one manually
- (void)_URLSession:(NSURLSession *)session downloadTaskDidStart:(NSURLSessionDownloadTask *)downloadTask {
    [self _taskDidStart:downloadTask];
}

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didResumeAtOffset:(int64_t)fileOffset expectedTotalBytes:(int64_t)expectedTotalBytes {
    [self _taskDidStart:downloadTask];
}

- (void)URLSession:(NSURLSession *)session downloadTask:(NSURLSessionDownloadTask *)downloadTask didFinishDownloadingToURL:(NSURL *)downloadURL {
    // try to get the file
    LDFile *file = [self _fileForIdentifier:[self _fileIdentifierForDownloadTask:downloadTask]];

    // store the file, only if we found a file
    BOOL success = file && [self _storeDataForFile:file dataURL:downloadURL];
    
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
    return [[LiveDepot sharedDepot] _dataForFile:self];
}

- (NSURL *)dataURL {
    // if the file exists
    if ([[LiveDepot sharedDepot] _dataExistsOnDiskForFile:self]) {
        // return the location
        return [[LiveDepot sharedDepot] _diskLocationForFile:self];
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

- (BOOL)containsFileWithExactMatch:(LDFile *)file {
    return [self any:^BOOL(id object) {
        return [((LDFile *)object) isEqualExactly:file];
    }];
}

- (BOOL)containsExactlyTheSameFilesAs:(NSArray *)files {
    // must be the same length
    if (self.count != files.count) return NO;

    // go though self
    for (LDFile *myFile in self) {

        // if the file isn't the correct class, throw exception
        if (![myFile isKindOfClass:LDFile.class]) @throw [NSException exceptionWithName:NSInvalidArgumentException reason:[NSString stringWithFormat:@"Object in receiver was not of type LDFile, instead it was: %@", NSStringFromClass(myFile.class)] userInfo:nil];
        
        // if the other array doesn't contain this file, return NO
        if (![files containsFileWithExactMatch:myFile]) return NO;
    }
    
    // if we got here it means they must be equal
    return YES;
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
        all files were downloaded, I updated a file name, then a download kept redownloading because for some reason it thought it was out of date (because the symlinking kept failing, I think because the file contents hadn't changed so the hash was the same, so the symlink already existed and the system didn't want to overwrite it), although why was it redownloading in the first palce if the sources hadn't changed, just the name? then after an app restart the file status was stuck in LDFileStatusAvailableAndDownloadingNewVersion and it wasn't getting budged. I should distinguish when the user quits the app, from when it's killed due to other reasons, and if the user quits it, I should cancel all downloads and clean up the file state. then on app restart I should make sure to do a recovery nonetheless.

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
