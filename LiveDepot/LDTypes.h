//
//  LDTypes.h
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

/**
 Represents the possible states a file can be in.
 */
typedef NS_ENUM(NSUInteger, LDFileStatus) {
    /** 
     The file isn't being downloaded and there is no data available either. 
     */
    LDFileStatusUnavailable,
    
    /** 
     The file is being downloaded, and there is no old data available. 
     */
    LDFileStatusDownloading,
    
    /** 
     The file is being updated, but the old data is still available. 
     */
    LDFileStatusAvailableAndDownloadingNewVersion,
    
    /** 
     The file has a new version available which is not being downloaded ATM, but old data is still available. 
     */
    LDFileStatusAvailableButOutOfDate,
    
    /** 
     The file has the latest data available. 
     */
    LDFileStatusAvailable,
};

typedef NS_ENUM(NSUInteger, LDFileUpdateType) {
    /**
     A new file with a never before seen identifier has been added to LiveDepot.
     */
    LDFileUpdateTypeNewFileAdded,
    
    /**
     This file has been known to LiveDepot, but some metadata has changed.
     */
    LDFileUpdateTypeMetadataChanged,
    
    /**
     The update was triggered manually by the client.
     */
    LDFileUpdateTypeManualTrigger,
    
    /**
     An inconsistency was detected in the file status and it was repaired as a result.
     */
    LDFileUpdateTypeRepairEvent,

    /**
     Download for this file has started.
     */
    LDFileUpdateTypeDownloadStarted,
    
    /**
     Some data was downloaded for this file.
     */
    LDFileUpdateTypeDownloadProgressChanged,
    
    /**
     The download failed. It will be retried soon.
     */
    LDFileUpdateTypeDownloadFailed,
    
    /**
     The download completed succesfully.
     */
    LDFileUpdateTypeDownloadSucceeded,
};

@class LDFile;

typedef void(^LDFileUpdatedBlock)(LDFile *file, LDFileUpdateType updateType);
typedef void(^LDFileListUpdatedBlock)(NSArray *files);
typedef void(^LDDownloadSchedulingCompletedBLock)(void);
typedef void(^LDWillScheduleDownloadsBlock)(NSUInteger toBeScheduledCount);

extern CGFloat const kLDDownloadProgressUnknown;
