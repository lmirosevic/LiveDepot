//
//  LDFile.h
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

#import <Foundation/Foundation.h>

#import "LDTypes.h"

@interface LDFile : NSObject <NSCoding>

// public
@property (copy, nonatomic) NSString                    *identifier;
@property (copy, nonatomic) NSString                    *name;
@property (copy, nonatomic) NSString                    *type;// file extension, e.g. @"pdf" or @"xlsx"
@property (strong, nonatomic) NSURL                     *thumbnail;// can be nil
@property (strong, nonatomic) NSArray                   *sources;// ordered list of sources where the file is available. If you pass in string objects, they can be NSStrings or NSURLs

/**
 Factory method
 */
+ (instancetype)fileWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources;

/**
 Designated initializer
 */
- (id)initWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources;

/**
 Checks if two files share the same identifier and are therefore treated as the same file
 */
- (BOOL)isEqual:(id)object;

/**
 Check if this file is exactly equal to another file, this means not just the identifier has to match, but everything else about this file as well including sources, name, thumbnail, etc.
 */
- (BOOL)isEqualExactly:(LDFile *)file;

@end

