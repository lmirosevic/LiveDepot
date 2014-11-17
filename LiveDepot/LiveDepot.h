//
//  LiveDepot.h
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

#import <Foundation/Foundation.h>

#import "LDTypes.h"
#import "LDFile.h"

#pragma mark - LiveDepot

/**
 Implements application:handleEventsForBackgroundURLSession:completionHandler with a hook for LiveDepot so it can integrte into the app background lifecycle.
 */
#define LiveDepotBackgroundURLSessionHook \
- (void)application:(UIApplication *)application handleEventsForBackgroundURLSession:(NSString *)identifier completionHandler:(void (^)())completionHandler { \
    [LiveDepot handleEventsForBackgroundURLSessionWithHandler:completionHandler]; \
}

@interface LiveDepot : NSObject

/**
 Returns a singleton object for the LiveDepot. Initialising this class using alloc/init is undefined.
 */
+ (LiveDepot *)sharedDepot;

/**
 You can call this to manually trigger a downloads sync.
 */
- (void)triggerDownloadsSync;

/**
 This method initialises our class with a flag that indicates that the download trigger should not be run on startup.
 */
+ (void)handleEventsForBackgroundURLSessionWithHandler:(void (^)())completionHandler;

/**
 Adds a file to the manager, so he will take care of synchronising it.
 */
- (void)addFile:(LDFile *)file;

/**
 Accessor for the files list. When setting the files list, the library will then compare them to the current files and add or remove as needed, triggering downloads and cleaning up old data appropriately.
 */
@property (strong, nonatomic) NSArray *files;

/**
 Method for setting the files, when you are interested in how many of those will result in new downloads being scheduled.
 */
- (void)setFiles:(NSArray *)files willScheduleDownloads:(LDWillScheduleDownloadsBlock)block;

/**
 Removes a file from the manager, this will cancel/fail any downloads, and remove it from disk, as well as the file list.
 */
- (void)removeFileWithIdentifier:(NSString *)fileIdentifier;

/**
 Removes a file from the manager, this will cancel/fail any downloads, and remove it from disk, as well as the file list.
 */
- (void)removeFile:(LDFile *)file;

/**
 Allows clients to register for updates on a particular file.
 */
- (void)addBlockForFileUpdatesForFile:(LDFile *)file withBlock:(LDFileUpdatedBlock)block inContext:(id)context;

/**
 Removes all the file update handlers for a given context.
 */
- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file inContext:(id)context;

/**
 Removes all the file update handlers for all contexts.
 */
- (void)removeAllBlocksForFileUpdatesForFile:(LDFile *)file;

/**
 Allows clients to register to receive updates on all files, as opposed to just a specific files.
 */
- (void)addBlockForWildcardFileUpdatesWithBlock:(LDFileUpdatedBlock)block inContext:(id)context;

/**
 Removes all the update blocks for wildcard file updates, for a given context.
 */
- (void)removeAllBlocksForFileWildcardUpdatesInContext:(id)context;

/**
 Manually triggers the update handlers to be called for a file and context.
 */
- (void)triggerBlockForFileUpdatesForFile:(LDFile *)file forContext:(id)context;

/**
 Allows clients to register for updates when the list of files changes.
 */
- (void)addBlockForFileListUpdates:(LDFileListUpdatedBlock)block inContext:(id)context;

/**
 Removes all the file list update handlers for a given context.
 */
- (void)removeAllBlocksForFileListUpdatesInContext:(id)context;

/**
 Removes all file list update handlers for all contexts.
 */
- (void)removeAllBlocksForFileListUpdates;

/**
 Manually triggers the update handlers to be called for the file list in a particular context.
 */
- (void)triggerBlockForFileListUpdatesForContext:(id)context;

/**
 You can register a block to be called when download scheduling has completed. When a new file is added to LiveDepot, first its sources are checked for reachability, each in turn, until a reachable source can be found, then this download is scheduled. When all of these "resolutions" conclude, and the download tasks have been handed off to the underlying NSURLSession, this block will fire.
 */
@property (copy, atomic) LDDownloadSchedulingCompletedBLock didCompleteDownloadSchedulingBlock;

/**
 A place to store the completion handler for the background download session.
 */
@property (copy, atomic) void(^backgroundSessionCompletionHandler)();

@end

#pragma mark - LDFile Category

@interface LDFile (LiveDepot)

/**
 The status of the file.
 */
@property (assign, nonatomic, readonly) LDFileStatus    status;

/**
 The download progress of the file.
 */
@property (assign, nonatomic, readonly) CGFloat         downloadProgress;

/**
 NSData object representing the file data.
 
 Returns nil when the file hasn't been downloaded yet.
 */
@property (strong, nonatomic, readonly) NSData          *data;

/**
 The URL for the actual data for the file.
 
 Returns nil when the file hasn't been downloaded yet.
 */
@property (strong, nonatomic, readonly) NSURL           *dataURL;

@end

#pragma mark - NSArray Category

@interface NSArray (LiveDepot)

/**
 Returns YES if the array contains a file with the identifier.
 */
- (BOOL)containsFileWithIdentifier:(NSString *)fileIdentifier;

/**
 Returns YES if the array contains the file using exactly equal semantics
 */
- (BOOL)containsFileWithExactMatch:(LDFile *)file;

/**
 Returns YES if the receiver and files array contains the same files, using exact comparison, order is ignored.
 */
- (BOOL)containsExactlyTheSameFilesAs:(NSArray *)files;

@end

