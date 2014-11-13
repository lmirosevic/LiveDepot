Pod::Spec.new do |s|
  s.name                  = 'LiveDepot'
  s.version               = '1.2.1'
  s.summary               = 'A library for simple file download and inventory mangement for iOS.'
  s.description           = 'A library for simple file download and inventory mangement for iOS. Including background transfers, mirrors, thumbnails, proper timeouts, persistent deferred retring, reachability integration, and an elegant blocks based API. It\'s NSURLSession for download tasks, on lean steroids.'
  s.homepage              = 'https://github.com/lmirosevic/LiveDepot'
  s.license               = { type: 'Apache License, Version 2.0', file: 'LICENSE' }
  s.author                = { 'Luka Mirosevic' => 'luka@goonbee.com' }
  s.source                = { git: 'https://github.com/lmirosevic/LiveDepot.git', tag: s.version.to_s }
  s.ios.deployment_target = '7.0'
  s.requires_arc          = true
  s.source_files          = 'LiveDepot/LiveDepot.{h,m}', 'LiveDepot/LDFile.{h,m}', 'LiveDepot/LDTypes.h'
  s.public_header_files   = 'LiveDepot/LiveDepot.h', 'LiveDepot/LDFile.h', 'LiveDepot/LDTypes.h'

  s.dependency 'GBStorage', '~> 2.2'
  s.dependency 'Reachability', '~> 3.1'

  s.dependency 'GBToolbox'
end
