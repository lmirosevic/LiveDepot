//
//  LDFile.m
//  KAICIID
//
//  Created by Luka Mirosevic on 04/11/2014.
//  Copyright (c) 2014 Luka Mirosevic. All rights reserved.
//

#import "LDFile.h"

@interface LDFile ()

// we create these properties here to synthesize an ivar and accessors for the class. We later expose the interface to these to LiveDepot in a private category
@property (assign, nonatomic, readwrite) BOOL                   hasSourceListChanged;
@property (assign, nonatomic, readwrite) BOOL                   isDataOutOfDate;

@end

@implementation LDFile

#pragma mark - CA

- (void)setSources:(NSArray *)sources {
    for (id url in sources) {
        if ([url isKindOfClass:NSURL.class]) {
            // noop, it's ok
        }
        else if ([url isKindOfClass:NSString.class]) {
            // make sure it's a valid URL
            if (!IsValidString(url) || ![NSURL URLWithString:url]) @throw [NSException exceptionWithName:NSInvalidArgumentException reason:[NSString stringWithFormat:@"URL passed in could not be parsed as a valid URL: %@", url] userInfo:nil];
        }
        else {
            // neither NSURL nor valid NSString
            @throw [NSException exceptionWithName:NSInvalidArgumentException reason:[NSString stringWithFormat:@"URL passed in was not of type NSURL or a valid URL NSString. You passed in an object of type %@ with description: %@", NSStringFromClass([url class]), [url description]] userInfo:nil];
        }
    }
    
    _sources = sources;
}

#pragma mark - API

+ (instancetype)fileWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources {
    return [[self alloc] initWithIdentifier:identifier name:name type:type thumbnail:thumbnail sources:sources];
}

- (id)initWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources {
    AssertParameterNotNil(identifier);
    AssertParameterNotNil(name);
    AssertParameterNotNil(type);
    AssertParameterNotEmptyArray(sources);
    
    return [self _initWithIdentifier:identifier name:name type:type thumbnail:thumbnail sources:sources hasSourceListChanged:NO isDataOutOfDate:NO];
}

- (BOOL)isEqualExactly:(LDFile *)file {
    return ([self isEqual:file] &&                                                              // identifier
            (self.name == file.name || [self.name isEqualToString:file.name]) &&                // name
            (self.type == file.type || [self.type isEqualToString:file.type]) &&                // type
            (self.thumbnail == file.thumbnail || [self.thumbnail isEqual:file.thumbnail]) &&    // thumbnail
            (self.sources == file.sources || [self.sources isEqual:file.sources]));             // sources
}

#pragma mark - Private

- (id)_initWithIdentifier:(NSString *)identifier name:(NSString *)name type:(NSString *)type thumbnail:(NSURL *)thumbnail sources:(NSArray *)sources hasSourceListChanged:(BOOL)hasSourceListChanged isDataOutOfDate:(BOOL)isDataOutOfDate {
    if (self = [super init]) {
        self.identifier = identifier;
        self.name = name;
        self.type = type;
        self.thumbnail = thumbnail;
        self.sources = sources;
        self.hasSourceListChanged = hasSourceListChanged;
        self.isDataOutOfDate = isDataOutOfDate;
    }
    
    return self;
}

#pragma mark - Overrides

- (NSString *)description {
    return [NSString stringWithFormat:@"<%@> [%@][%@] \"%@\" %.2f%% s%lu (%@) (%lu sources)", NSStringFromClass(self.class), self.identifier, self.type, self.name, self.downloadProgress * 100., self.status, [self.thumbnail absoluteString], (unsigned long)self.sources.count];
}

- (NSUInteger)hash {
    return [self.identifier hash];
}

- (BOOL)isEqual:(id)object {
    return [object isKindOfClass:LDFile.class] && [self.identifier isEqualToString:((LDFile *)object).identifier];
}

#pragma mark - NSCoding

- (void)encodeWithCoder:(NSCoder *)coder {
    [coder encodeObject:self.identifier forKey:@"identifier"];
    [coder encodeObject:self.name forKey:@"name"];
    [coder encodeObject:self.type forKey:@"type"];
    [coder encodeObject:self.thumbnail forKey:@"thumbnail"];
    [coder encodeObject:self.sources forKey:@"sources"];
    [coder encodeBool:self.hasSourceListChanged forKey:@"hasSourceListChanged"];
    [coder encodeBool:self.isDataOutOfDate forKey:@"isDataOutOfDate"];
}

- (id)initWithCoder:(NSCoder *)coder {
    return [self _initWithIdentifier:[coder decodeObjectForKey:@"identifier"]
                                name:[coder decodeObjectForKey:@"name"]
                                type:[coder decodeObjectForKey:@"type"]
                           thumbnail:[coder decodeObjectForKey:@"thumbnail"]
                             sources:[coder decodeObjectForKey:@"sources"]
                hasSourceListChanged:[coder decodeBoolForKey:@"hasSourceListChanged"]
                     isDataOutOfDate:[coder decodeBoolForKey:@"isDataOutOfDate"]];
}

@end
