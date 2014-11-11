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
typedef enum : NSUInteger {
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
} LDFileStatus;

@class LDFile;

typedef void(^LDFileUpdatedBlock)(LDFile *file);
typedef void(^LDFileListUpdatedBlock)(NSArray *files);

extern CGFloat const kLDDownloadProgressUnknown;
