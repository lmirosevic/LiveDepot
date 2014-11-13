//
//  LDFile.h
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <QuickLook/QuickLook.h>

#import "LDTypes.h"

@interface LDFile : NSObject <NSCoding, NSCopying, QLPreviewItem>

/**
 The unique file identifier.
 */
@property (copy, nonatomic) NSString                    *identifier;

/**
 The file name as it will be displayed
 */
@property (copy, nonatomic) NSString                    *name;

/**
 File extension, e.g. @"pdf" or @"xlsx".
 */
@property (copy, nonatomic) NSString                    *type;


/**
 Thumbnail location for this file. 
 
 Can be nil.
 */
@property (strong, nonatomic) NSURL                     *thumbnail;

/**
 Ordered list of sources where the file is available. If you pass in string objects, they can be NSStrings or NSURLs.
 */
@property (strong, nonatomic) NSArray                   *sources;

/**
 A wildcard meta dictionary for the user to store his own properties in that may be useful to have associated with the file.
 
 The dictionary tree objects must all conform to NSCoding. This dictionary must get serialised to disk quite prequently for consistency purposes, so for performance reasons keep this dictionary as light as possible.
 
 Can be nil.
 */
@property (strong, nonatomic) NSDictionary              *meta;

/**
 Factory method
 */
+ (instancetype)fileWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources meta:(NSDictionary *)meta;

/**
 Designated initializer
 */
- (id)initWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources meta:(NSDictionary *)meta;

/**
 Checks if two files share the same identifier and are therefore treated as the same file
 */
- (BOOL)isEqual:(id)object;

/**
 Check if this file is exactly equal to another file, this means not just the identifier has to match, but everything else about this file as well including sources, name, thumbnail, etc.
 */
- (BOOL)isEqualExactly:(LDFile *)file;

@end

