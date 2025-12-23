// Auto-generated registry extracted for runtime reuse
export const toolDefinitionMap = new Map([

  ["create-relation", {
    name: "create-relation",
    description: `Create Relation.Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-293776/cd0f4122abc04d3`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Your Sub Account's ID"},"associationId":{"type":"string","description":"Association's Id"},"firstRecordId":{"type":"string","description":"First Record's Id. For instance, if you have an association between a contact and a custom object, and you specify the contact as the first object while creating the association, then your firstRecordId would be the contactId "},"secondRecordId":{"type":"string","description":"Second Record's Id.For instance, if you have an association between a contact and a custom object, and you specify the custom object as the second entity while creating the association, then your secondRecordId would be the customObject record Id"}},"required":["locationId","associationId","firstRecordId","secondRecordId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/associations/relations",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["associations/relation.write"]}]
  }],
  ["get-relations-by-record-id", {
    name: "get-relations-by-record-id",
    description: `Get all relations by record Id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"recordId":{"type":"string"},"locationId":{"type":"string","description":"Your Sub Account's ID"},"skip":{"type":"number"},"limit":{"type":"number"},"associationIds":{"type":"array","items":{"type":"string"},"description":"Association Ids"}},"required":["Version","recordId","locationId","skip","limit"]},
    method: "get",
    pathTemplate: "/associations/relations/{recordId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"recordId","in":"path"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"associationIds","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations/relation.readonly"]}]
  }],
  ["get-association-key-by-key-name", {
    name: "get-association-key-by-key-name",
    description: `Using this api you can get standard / user defined association by key`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"key_name":{"type":"string"},"locationId":{"type":"string"}},"required":["Version","key_name","locationId"]},
    method: "get",
    pathTemplate: "/associations/key/{key_name}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"key_name","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations.readonly"]}]
  }],
  ["get-association-by-object-keys", {
    name: "get-association-by-object-keys",
    description: `Get association by object keys like contacts, custom objects and opportunities. Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-293776/cd0f4122abc04d3`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"objectKey":{"type":"string"},"locationId":{"type":"string"}},"required":["Version"]},
    method: "get",
    pathTemplate: "/associations/objectKey/{objectKey}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"objectKey","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations.readonly"]}]
  }],
  ["get-association-by-ID", {
    name: "get-association-by-ID",
    description: `Using this api you can get SYSTEM_DEFINED / USER_DEFINED association by id `,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"associationId":{"type":"string"}},"required":["Version","associationId"]},
    method: "get",
    pathTemplate: "/associations/{associationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"associationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations.readonly"]}]
  }],
  ["update-association", {
    name: "update-association",
    description: `Update Association , Allows you to update labels of an associations. Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-293776/cd0f4122abc04d3`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"associationId":{"type":"string"},"requestBody":{"type":"object","properties":{"firstObjectLabel":{"type":"object"},"secondObjectLabel":{"type":"object"}},"required":["firstObjectLabel","secondObjectLabel"],"description":"The JSON request body."}},"required":["Version","associationId","requestBody"]},
    method: "put",
    pathTemplate: "/associations/{associationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"associationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["associations.write"]}]
  }],
  ["delete-association", {
    name: "delete-association",
    description: `Delete USER_DEFINED Association By Id, deleting an association will also all the relations for that association`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"associationId":{"type":"string"}},"required":["Version","associationId"]},
    method: "delete",
    pathTemplate: "/associations/{associationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"associationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations.write"]}]
  }],
  ["check-url-slug-exists", {
    name: "check-url-slug-exists",
    description: `The "Check url slug" API allows check the blog slug validation which is needed before publishing any blog post. Please use blogs/check-slug.readonly. you can find the POST ID from the post edit url.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"urlSlug":{"type":"string"},"locationId":{"type":"string"},"postId":{"type":"string"}},"required":["Version","urlSlug","locationId"]},
    method: "get",
    pathTemplate: "/blogs/posts/url-slug-exists",
    executionParameters: [{"name":"Version","in":"header"},{"name":"urlSlug","in":"query"},{"name":"locationId","in":"query"},{"name":"postId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["blogs/check-slug.readonly"]}]
  }],
  ["update-blog-post", {
    name: "update-blog-post",
    description: `The "Update Blog Post" API allows you update blog post for any given blog site. Please use blogs/post-update.write`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"postId":{"type":"string"},"requestBody":{"type":"object","properties":{"title":{"type":"string"},"locationId":{"type":"string"},"blogId":{"type":"string","description":"You can find the blog id from blog site dashboard link"},"imageUrl":{"type":"string"},"description":{"type":"string"},"rawHTML":{"type":"string"},"status":{"type":"string","enum":["DRAFT","PUBLISHED","SCHEDULED","ARCHIVED"]},"imageAltText":{"type":"string"},"categories":{"type":"array","items":{"type":"string"},"description":"This needs to be array of category ids, which you can get from the category get api call."},"tags":{"type":"array","items":{"type":"string"}},"author":{"type":"string","description":"This needs to be author id, which you can get from the author get api call."},"urlSlug":{"type":"string"},"canonicalLink":{"type":"string"},"publishedAt":{"type":"string","description":"Provide ISO timestamp"}},"required":["title","locationId","blogId","imageUrl","description","rawHTML","status","imageAltText","categories","author","urlSlug","publishedAt"],"description":"The JSON request body."}},"required":["Version","postId","requestBody"]},
    method: "put",
    pathTemplate: "/blogs/posts/{postId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"postId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["blogs/post-update.write"]}]
  }],
  ["create-blog-post", {
    name: "create-blog-post",
    description: `The "Create Blog Post" API allows you create blog post for any given blog site. Please use blogs/post.write`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"title":{"type":"string"},"locationId":{"type":"string"},"blogId":{"type":"string","description":"You can find the blog id from blog site dashboard link"},"imageUrl":{"type":"string"},"description":{"type":"string"},"rawHTML":{"type":"string"},"status":{"type":"string","enum":["DRAFT","PUBLISHED","SCHEDULED","ARCHIVED"]},"imageAltText":{"type":"string"},"categories":{"type":"array","items":{"type":"string"},"description":"This needs to be array of category ids, which you can get from the category get api call."},"tags":{"type":"array","items":{"type":"string"}},"author":{"type":"string","description":"This needs to be author id, which you can get from the author get api call."},"urlSlug":{"type":"string"},"canonicalLink":{"type":"string"},"publishedAt":{"type":"string","description":"Provide ISO timestamp"}},"required":["title","locationId","blogId","imageUrl","description","rawHTML","status","imageAltText","categories","author","urlSlug","publishedAt"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/blogs/posts",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["blogs/post.write"]}]
  }],
  ["get-all-blog-authors-by-location", {
    name: "get-all-blog-authors-by-location",
    description: `The "Get all authors" Api return the blog authors for a given location ID. Please use "blogs/author.readonly" `,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"limit":{"type":"number","description":"Number of authors to show in the listing"},"offset":{"type":"number","description":"Number of authors to skip in listing"}},"required":["Version","locationId","limit","offset"]},
    method: "get",
    pathTemplate: "/blogs/authors",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["blogs/author.readonly"]}]
  }],
  ["get-all-categories-by-location", {
    name: "get-all-categories-by-location",
    description: `The "Get all categories" Api return the blog categoies for a given location ID. Please use "blogs/category.readonly" `,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"limit":{"type":"number","description":"Number of categories to show in the listing"},"offset":{"type":"number","description":"Number of categories to skip in listing"}},"required":["Version","locationId","limit","offset"]},
    method: "get",
    pathTemplate: "/blogs/categories",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["blogs/category.readonly"]}]
  }],
  ["get-blog-post", {
    name: "get-blog-post",
    description: `The "Get Blog posts by Blog ID" API allows you get blog posts for any given blog site using blog ID.Please use blogs/posts.readonly`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"blogId":{"type":"string"},"limit":{"type":"number"},"offset":{"type":"number"},"searchTerm":{"type":"string","description":"search for any post by name"},"status":{"enum":["PUBLISHED","SCHEDULED","ARCHIVED","DRAFT"],"type":"string"}},"required":["Version","locationId","blogId","limit","offset"]},
    method: "get",
    pathTemplate: "/blogs/posts/all",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"blogId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"searchTerm","in":"query"},{"name":"status","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["blogs/posts.readonly"]}]
  }],
  ["get-blogs", {
    name: "get-blogs",
    description: `The "Get Blogs by Location ID" API allows you get blogs using Location ID.Please use blogs/list.readonly`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"skip":{"type":"number"},"limit":{"type":"number"},"searchTerm":{"type":"string","description":"search for any post by name"}},"required":["Version","locationId","skip","limit"]},
    method: "get",
    pathTemplate: "/blogs/site/all",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"searchTerm","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["blogs/list.readonly"]}]
  }],
  ["get-business", {
    name: "get-business",
    description: `Get Business`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"businessId":{"type":"string"}},"required":["Version","businessId"]},
    method: "get",
    pathTemplate: "/businesses/{businessId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"businessId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["businesses.readonly"]}]
  }],
  ["update-business", {
    name: "update-business",
    description: `Update Business`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"businessId":{"type":"string"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"phone":{"type":"string"},"email":{"type":"string"},"postalCode":{"type":"string"},"website":{"type":"string"},"address":{"type":"string"},"state":{"type":"string"},"city":{"type":"string"},"country":{"type":"string"},"description":{"type":"string"}},"description":"The JSON request body."}},"required":["Version","businessId","requestBody"]},
    method: "put",
    pathTemplate: "/businesses/{businessId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"businessId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["businesses.write"]}]
  }],
  ["delete-Business", {
    name: "delete-Business",
    description: `Delete Business`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"businessId":{"type":"string"}},"required":["Version","businessId"]},
    method: "delete",
    pathTemplate: "/businesses/{businessId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"businessId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["businesses.write"]}]
  }],
  ["get-groups", {
    name: "get-groups",
    description: `Get all calendar groups in a location.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/calendars/groups",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/groups.readonly"]}]
  }],
  ["create-calendar-group", {
    name: "create-calendar-group",
    description: `Create Calendar Group`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"slug":{"type":"string"},"isActive":{"type":"boolean"}},"required":["locationId","name","description","slug"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/groups",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/groups.write"]}]
  }],
  ["validate-groups-slug", {
    name: "validate-groups-slug",
    description: `Validate if group slug is available or not.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location Id"},"slug":{"type":"string","description":"Slug"}},"required":["locationId","slug"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/groups/validate-slug",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/groups.write"]}]
  }],
  ["edit-group", {
    name: "edit-group",
    description: `Update Group by group ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"groupId":{"type":"string","description":"Group Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"description":{"type":"string"},"slug":{"type":"string"}},"required":["name","description","slug"],"description":"The JSON request body."}},"required":["Version","groupId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/groups/{groupId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"groupId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/groups.write"]}]
  }],
  ["delete-group", {
    name: "delete-group",
    description: `Delete Group`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"groupId":{"type":"string","description":"Group Id"}},"required":["Version","groupId"]},
    method: "delete",
    pathTemplate: "/calendars/groups/{groupId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"groupId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/groups.write"]}]
  }],
  ["disable-group", {
    name: "disable-group",
    description: `Disable Group`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"groupId":{"type":"string","description":"Group Id"},"requestBody":{"type":"object","properties":{"isActive":{"type":"boolean","description":"Is Active?"}},"required":["isActive"],"description":"The JSON request body."}},"required":["Version","groupId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/groups/{groupId}/status",
    executionParameters: [{"name":"Version","in":"header"},{"name":"groupId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/groups.write"]}]
  }],
  ["create-appointment", {
    name: "create-appointment",
    description: `Create appointment`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title"},"meetingLocationType":{"type":"string","description":"Meeting location type. \n- If `address` is provided in the request body, the `meetingLocationType` defaults to **custom**.","enum":["custom","zoom","gmeet","phone","address","ms_teams","google"]},"meetingLocationId":{"type":"string","description":"The unique identifier for the meeting location.\n- This value can be found in `calendar.locationConfigurations`or `calendar.teamMembers[].locationConfigurations`","default":"default"},"overrideLocationConfig":{"type":"boolean","description":"Flag to override location config\n- **false** - If only `meetingLocationId` is provided\n- **true** - If only `meetingLocationType` is provided\n"},"appointmentStatus":{"type":"string","enum":["new","confirmed","cancelled","showed","noshow","invalid"]},"assignedUserId":{"type":"string","description":"Assigned User Id"},"address":{"type":"string","description":"Appointment Address"},"ignoreDateRange":{"type":"boolean","description":"If set to true, the minimum scheduling notice and date range would be ignored"},"toNotify":{"type":"boolean","description":"If set to false, the automations will not run"},"ignoreFreeSlotValidation":{"type":"boolean","description":"If true the time slot validation would be avoided for any appointment creation (even the ignoreDateRange)"},"rrule":{"type":"string","description":"RRULE as per the iCalendar (RFC 5545) specification for recurring events. DTSTART is not required, instance ids are calculated on the basis of startTime of the event. The rrule only be applied if ignoreFreeSlotValidation is true."},"calendarId":{"type":"string","description":"Calendar Id"},"locationId":{"type":"string","description":"Location Id"},"contactId":{"type":"string","description":"Contact Id"},"startTime":{"type":"string","description":"Start Time"},"endTime":{"type":"string","description":"End Time"}},"required":["calendarId","locationId","contactId","startTime"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/events/appointments",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["get-appointment", {
    name: "get-appointment",
    description: `Get appointment by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"eventId":{"type":"string","description":"Event Id or Instance id. For recurring appointments send masterEventId to modify original series."}},"required":["Version","eventId"]},
    method: "get",
    pathTemplate: "/calendars/events/appointments/{eventId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"eventId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/events.readonly"]}]
  }],
  ["edit-appointment", {
    name: "edit-appointment",
    description: `Update appointment`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"eventId":{"type":"string","description":"Event Id or Instance id. For recurring appointments send masterEventId to modify original series."},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title"},"meetingLocationType":{"type":"string","description":"Meeting location type. \n- If `address` is provided in the request body, the `meetingLocationType` defaults to **custom**.","enum":["custom","zoom","gmeet","phone","address","ms_teams","google"]},"meetingLocationId":{"type":"string","description":"The unique identifier for the meeting location.\n- This value can be found in `calendar.locationConfigurations`or `calendar.teamMembers[].locationConfigurations`","default":"default"},"overrideLocationConfig":{"type":"boolean","description":"Flag to override location config\n- **false** - If only `meetingLocationId` is provided\n- **true** - If only `meetingLocationType` is provided\n"},"appointmentStatus":{"type":"string","enum":["new","confirmed","cancelled","showed","noshow","invalid"]},"assignedUserId":{"type":"string","description":"Assigned User Id"},"address":{"type":"string","description":"Appointment Address"},"ignoreDateRange":{"type":"boolean","description":"If set to true, the minimum scheduling notice and date range would be ignored"},"toNotify":{"type":"boolean","description":"If set to false, the automations will not run"},"ignoreFreeSlotValidation":{"type":"boolean","description":"If true the time slot validation would be avoided for any appointment creation (even the ignoreDateRange)"},"rrule":{"type":"string","description":"RRULE as per the iCalendar (RFC 5545) specification for recurring events. DTSTART is not required, instance ids are calculated on the basis of startTime of the event. The rrule only be applied if ignoreFreeSlotValidation is true."},"calendarId":{"type":"string","description":"Calendar Id"},"startTime":{"type":"string","description":"Start Time"},"endTime":{"type":"string","description":"End Time"}},"description":"The JSON request body."}},"required":["Version","eventId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/events/appointments/{eventId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"eventId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["get-calendar-events", {
    name: "get-calendar-events",
    description: `Get Calendar Events`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id - Owner of an appointment. Either of userId, groupId or calendarId is required"},"calendarId":{"type":"string","description":"Either of calendarId, userId or groupId is required"},"groupId":{"type":"string","description":"Either of groupId, calendarId or userId is required"},"startTime":{"type":"string","description":"Start Time (in millis)"},"endTime":{"type":"string","description":"End Time (in millis)"}},"required":["Version","locationId","startTime","endTime"]},
    method: "get",
    pathTemplate: "/calendars/events",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"calendarId","in":"query"},{"name":"groupId","in":"query"},{"name":"startTime","in":"query"},{"name":"endTime","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/events.readonly"]}]
  }],
  ["get-blocked-slots", {
    name: "get-blocked-slots",
    description: `Get Blocked Slots`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id - Owner of an appointment. Either of userId, groupId or calendarId is required"},"calendarId":{"type":"string","description":"Either of calendarId, userId or groupId is required"},"groupId":{"type":"string","description":"Either of groupId, calendarId or userId is required"},"startTime":{"type":"string","description":"Start Time (in millis)"},"endTime":{"type":"string","description":"End Time (in millis)"}},"required":["Version","locationId","startTime","endTime"]},
    method: "get",
    pathTemplate: "/calendars/blocked-slots",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"calendarId","in":"query"},{"name":"groupId","in":"query"},{"name":"startTime","in":"query"},{"name":"endTime","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/events.readonly"]}]
  }],
  ["create-block-slot", {
    name: "create-block-slot",
    description: `Create block slot`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title"},"calendarId":{"type":"string","description":"Either calendarId or assignedUserId can be set, not both."},"assignedUserId":{"type":"string","description":"Either calendarId or assignedUserId can be set, not both."},"locationId":{"type":"string","description":"Location Id"},"startTime":{"type":"string","description":"Start Time"},"endTime":{"type":"string","description":"End Time"}},"required":["calendarId","locationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/events/block-slots",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["edit-block-slot", {
    name: "edit-block-slot",
    description: `Update block slot by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"eventId":{"type":"string","description":"Event Id or Instance id. For recurring appointments send masterEventId to modify original series."},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title"},"calendarId":{"type":"string","description":"Either calendarId or assignedUserId can be set, not both."},"assignedUserId":{"type":"string","description":"Either calendarId or assignedUserId can be set, not both."},"locationId":{"type":"string","description":"Location Id"},"startTime":{"type":"string","description":"Start Time"},"endTime":{"type":"string","description":"End Time"}},"required":["calendarId","locationId"],"description":"The JSON request body."}},"required":["Version","eventId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/events/block-slots/{eventId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"eventId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["get-slots", {
    name: "get-slots",
    description: `Get free slots for a calendar between a date range. Optionally a consumer can also request free slots in a particular timezone and also for a particular user.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string","description":"Calendar Id"},"startDate":{"type":"number","description":"Start Date (**⚠️ Important:** Date range cannot be more than 31 days)"},"endDate":{"type":"number","description":"End Date (**⚠️ Important:** Date range cannot be more than 31 days)"},"timezone":{"type":"string","description":"The timezone in which the free slots are returned"},"userId":{"type":"string","description":"The user for whom the free slots are returned"},"userIds":{"type":"array","items":{"type":"string"},"description":"The users for whom the free slots are returned"}},"required":["Version","calendarId","startDate","endDate"]},
    method: "get",
    pathTemplate: "/calendars/{calendarId}/free-slots",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"},{"name":"startDate","in":"query"},{"name":"endDate","in":"query"},{"name":"timezone","in":"query"},{"name":"userId","in":"query"},{"name":"userIds","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars.readonly"]}]
  }],
  ["get-calendar", {
    name: "get-calendar",
    description: `Get calendar by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string","description":"Calendar Id"}},"required":["Version","calendarId"]},
    method: "get",
    pathTemplate: "/calendars/{calendarId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars.readonly"]}]
  }],
  ["update-calendar", {
    name: "update-calendar",
    description: `Update calendar by ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string","description":"Calendar Id"},"requestBody":{"type":"object","properties":{"notifications":{"description":"🚨 Deprecated! Please use 'Calendar Notifications APIs' instead.","type":"array","items":{"type":"object","properties":{"type":{"type":"string","description":"Calendar Notification","enum":["email"],"default":"email"},"shouldSendToContact":{"type":"boolean"},"shouldSendToGuest":{"type":"boolean"},"shouldSendToUser":{"type":"boolean"},"shouldSendToSelectedUsers":{"type":"boolean"},"selectedUsers":{"type":"string","description":"Comma separated emails"}},"required":["shouldSendToContact","shouldSendToGuest","shouldSendToUser","shouldSendToSelectedUsers","selectedUsers"]}},"groupId":{"type":"string","description":"Group Id"},"teamMembers":{"description":"Team members are required for calendars of type: Round Robin, Collective, Class, Service. Personal calendar must have exactly one team member.","type":"array","items":{"type":"object","properties":{"userId":{"type":"string"},"priority":{"type":"number","default":0.5,"enum":[0,0.5,1]},"meetingLocationType":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.kind` instead.","default":"custom","enum":["custom","zoom","gmeet","phone","address","teams","booker"]},"meetingLocation":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.location` instead."},"isPrimary":{"type":"boolean","description":"Marks a user as primary. This property is required in case of collective booking calendars. Only one user can be primary."},"locationConfigurations":{"description":"Meeting location configuration for event calendar.\n- *Multiple locations are allowed only when one team member is selected.*\n- *For **Class booking** and **Collective** calendars, only one location configuration is allowed for each team member.*","type":"array","items":{"type":"object","properties":{"kind":{"type":"string","description":"Type of meeting location. zoom_conference/google_conference/ms_teams_conference is not supported in event calendar type","enum":["custom","zoom_conference","google_conference","inbound_call","outbound_call","physical","booker","ms_teams_conference"]},"location":{"type":"string","description":"Address for meeting location. Not applicable on \"zoom_conference\", \"google_conference\" and \"ms_teams_conference\" kind"}},"required":["kind"]}}},"required":["userId"]}},"eventType":{"type":"string","enum":["RoundRobin_OptimizeForAvailability","RoundRobin_OptimizeForEqualDistribution"]},"name":{"type":"string"},"description":{"type":"string"},"slug":{"type":"string"},"widgetSlug":{"type":"string"},"widgetType":{"type":"string","description":"Calendar widget type. Choose \"default\" for \"neo\" and \"classic\" for \"classic\" layout.","enum":["default","classic"],"default":"classic"},"eventTitle":{"type":"string"},"eventColor":{"type":"string","default":"#039be5"},"locationConfigurations":{"description":"Meeting location configuration for event calendar","type":"array","items":{"type":"object","properties":{"kind":{"type":"string","description":"Type of meeting location. zoom_conference/google_conference/ms_teams_conference is not supported in event calendar type","enum":["custom","zoom_conference","google_conference","inbound_call","outbound_call","physical","booker","ms_teams_conference"]},"location":{"type":"string","description":"Address for meeting location. Not applicable on \"zoom_conference\", \"google_conference\" and \"ms_teams_conference\" kind"}},"required":["kind"]}},"meetingLocation":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.location` or `teamMembers[].locationConfigurations.location` instead."},"slotDuration":{"type":"number","description":"This controls the duration of the meeting","default":30},"slotDurationUnit":{"type":"string","description":"Unit for slot duration.","enum":["mins","hours"]},"preBufferUnit":{"type":"string","description":"Unit for pre-buffer.","enum":["mins","hours"]},"slotInterval":{"type":"number","description":"Slot interval reflects the amount of time the between booking slots that will be shown in the calendar.","default":30},"slotIntervalUnit":{"type":"string","description":"Unit for slot interval.","enum":["mins","hours"]},"slotBuffer":{"type":"number","description":"Slot-Buffer is additional time that can be added after an appointment, allowing for extra time to wrap up"},"preBuffer":{"type":"number","description":"Pre-Buffer is additional time that can be added before an appointment, allowing for extra time to get ready"},"appoinmentPerSlot":{"type":"number"},"appoinmentPerDay":{"type":"number","description":"Number of appointments that can be booked for a given day"},"allowBookingAfter":{"type":"number","description":"Minimum scheduling notice for events"},"allowBookingAfterUnit":{"type":"string","description":"Unit for minimum scheduling notice","enum":["hours","days","weeks","months"]},"allowBookingFor":{"type":"number","description":"Minimum number of days/weeks/months for which to allow booking events"},"allowBookingForUnit":{"type":"string","description":"Unit for controlling the duration for which booking would be allowed for","enum":["days","weeks","months"]},"openHours":{"type":"array","items":{"type":"object","properties":{"daysOfTheWeek":{"type":"array","items":{"type":"number","maximum":6,"minimum":0}},"hours":{"type":"array","items":{"type":"object","properties":{"openHour":{"type":"number","minimum":0,"maximum":23},"openMinute":{"type":"number","minimum":0,"maximum":60},"closeHour":{"type":"number","minimum":0,"maximum":23},"closeMinute":{"type":"number","minimum":0,"maximum":60}},"required":["openHour","openMinute","closeHour","closeMinute"]}}},"required":["daysOfTheWeek","hours"]}},"enableRecurring":{"type":"boolean","description":"Enable recurring appointments for the calendars. Please note that only one member should be added in the calendar to enable this","default":false},"recurring":{"type":"object","properties":{"freq":{"type":"string","enum":["DAILY","WEEKLY","MONTHLY"]},"count":{"type":"number","description":"Number of recurrences","maximum":24},"bookingOption":{"type":"string","description":"This setting contols what to do incase a recurring slot is unavailable","enum":["skip","continue","book_next"]},"bookingOverlapDefaultStatus":{"type":"string","description":"This setting contols what to do incase a recurring slot is unavailable","enum":["confirmed","new"]}}},"formId":{"type":"string"},"stickyContact":{"type":"boolean"},"isLivePaymentMode":{"type":"boolean"},"autoConfirm":{"type":"boolean"},"shouldSendAlertEmailsToAssignedMember":{"type":"boolean"},"alertEmail":{"type":"string"},"googleInvitationEmails":{"type":"boolean"},"allowReschedule":{"type":"boolean"},"allowCancellation":{"type":"boolean"},"shouldAssignContactToTeamMember":{"type":"boolean"},"shouldSkipAssigningContactForExisting":{"type":"boolean"},"notes":{"type":"string"},"pixelId":{"type":"string"},"formSubmitType":{"type":"string","default":"ThankYouMessage","enum":["RedirectURL","ThankYouMessage"]},"formSubmitRedirectURL":{"type":"string"},"formSubmitThanksMessage":{"type":"string"},"availabilityType":{"type":"number","description":"Determines which availability type to consider:\n- **1**: Only custom availabilities will be used.\n- **0**: Only open hours will be used.\n- **null**: Both the custom availabilities and open hours will be considered.","enum":[0,1]},"availabilities":{"description":"This is only to set the custom availability. For standard availability, use the openHours property","type":"array","items":{"type":"object","properties":{"date":{"type":"string","description":"Formulate the date string in the format of `<YYYY-MM-DD in local timezone>T00:00:00.000Z`."},"hours":{"type":"array","items":{"type":"object","properties":{"openHour":{"type":"number","minimum":0,"maximum":23},"openMinute":{"type":"number","minimum":0,"maximum":60},"closeHour":{"type":"number","minimum":0,"maximum":23},"closeMinute":{"type":"number","minimum":0,"maximum":60}},"required":["openHour","openMinute","closeHour","closeMinute"]}},"deleted":{"type":"boolean","default":false},"id":{"type":"string","description":"The ID of the custom availability object. It is required while updating or deleting the existing custom date availability"}},"required":["date","hours"]}},"guestType":{"type":"string","enum":["count_only","collect_detail"]},"consentLabel":{"type":"string"},"calendarCoverImage":{"type":"string"},"lookBusyConfig":{"description":"Look Busy Configuration","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Apply Look Busy","example":true,"default":false},"LookBusyPercentage":{"type":"number","description":"Percentage of slots that will be hidden"}},"required":["enabled","LookBusyPercentage"]}]},"isActive":{"type":"boolean"}},"description":"The JSON request body."}},"required":["Version","calendarId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/{calendarId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars.write"]}]
  }],
  ["delete-calendar", {
    name: "delete-calendar",
    description: `Delete calendar by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string","description":"Calendar Id"}},"required":["Version","calendarId"]},
    method: "delete",
    pathTemplate: "/calendars/{calendarId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars.write"]}]
  }],
  ["delete-event", {
    name: "delete-event",
    description: `Delete event by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"eventId":{"type":"string","description":"Event Id or Instance id. For recurring appointments send masterEventId to modify original series."},"requestBody":{"type":"object","properties":{},"description":"The JSON request body."}},"required":["Version","eventId","requestBody"]},
    method: "delete",
    pathTemplate: "/calendars/events/{eventId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"eventId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["get-appointment-notes", {
    name: "get-appointment-notes",
    description: `Get Appointment Notes`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"limit":{"maximum":20,"type":"number","description":"Limit of notes to fetch"},"offset":{"minimum":0,"type":"number","description":"Offset of notes to fetch"},"appointmentId":{"type":"string","description":"Appointment ID"}},"required":["Version","limit","offset","appointmentId"]},
    method: "get",
    pathTemplate: "/calendars/appointments/{appointmentId}/notes",
    executionParameters: [{"name":"Version","in":"header"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"appointmentId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/events.readonly"]}]
  }],
  ["create-appointment-note", {
    name: "create-appointment-note",
    description: `Create Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"appointmentId":{"type":"string","description":"Appointment ID"},"requestBody":{"type":"object","properties":{"userId":{"type":"string"},"body":{"type":"string"}},"required":["body"],"description":"The JSON request body."}},"required":["Version","appointmentId","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/appointments/{appointmentId}/notes",
    executionParameters: [{"name":"Version","in":"header"},{"name":"appointmentId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["update-appointment-note", {
    name: "update-appointment-note",
    description: `Update Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"appointmentId":{"type":"string","description":"Appointment ID"},"noteId":{"type":"string"},"requestBody":{"type":"object","properties":{"userId":{"type":"string"},"body":{"type":"string"}},"required":["body"],"description":"The JSON request body."}},"required":["Version","appointmentId","noteId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/appointments/{appointmentId}/notes/{noteId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"appointmentId","in":"path"},{"name":"noteId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-appointment-note", {
    name: "delete-appointment-note",
    description: `Delete Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"appointmentId":{"type":"string","description":"Appointment ID"},"noteId":{"type":"string"}},"required":["Version","appointmentId","noteId"]},
    method: "delete",
    pathTemplate: "/calendars/appointments/{appointmentId}/notes/{noteId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"appointmentId","in":"path"},{"name":"noteId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-calendar-resource", {
    name: "get-calendar-resource",
    description: `Get calendar resource by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"resourceType":{"enum":["equipments","rooms"],"type":"string","description":"Calendar Resource Type"},"id":{"type":"string","description":"Calendar Resource ID"}},"required":["Version","resourceType","id"]},
    method: "get",
    pathTemplate: "/calendars/resources/{resourceType}/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"resourceType","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["update-calendar-resource", {
    name: "update-calendar-resource",
    description: `Update calendar resource by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"resourceType":{"enum":["equipments","rooms"],"type":"string","description":"Calendar Resource Type"},"id":{"type":"string","description":"Calendar Resource ID"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"quantity":{"type":"number","description":"Quantity of the equipment."},"outOfService":{"type":"number","description":"Quantity of the out of service equipment."},"capacity":{"type":"number","description":"Capacity of the room."},"calendarIds":{"description":"Service calendar IDs to be mapped with the resource.\n\n    One equipment can only be mapped with one service calendar.\n    \nOne room can be mapped with multiple service calendars.","maxItems":100,"type":"array","items":{"type":"string"}},"isActive":{"type":"boolean"}},"description":"The JSON request body."}},"required":["Version","resourceType","id","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/resources/{resourceType}/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"resourceType","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["delete-calendar-resource", {
    name: "delete-calendar-resource",
    description: `Delete calendar resource by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"resourceType":{"enum":["equipments","rooms"],"type":"string","description":"Calendar Resource Type"},"id":{"type":"string","description":"Calendar Resource ID"}},"required":["Version","resourceType","id"]},
    method: "delete",
    pathTemplate: "/calendars/resources/{resourceType}/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"resourceType","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["fetch-calendar-resources", {
    name: "fetch-calendar-resources",
    description: `List calendar resources by resource type and location ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"resourceType":{"enum":["equipments","rooms"],"type":"string","description":"Calendar Resource Type"},"locationId":{"type":"string"},"limit":{"type":"number"},"skip":{"type":"number"}},"required":["Version","resourceType","locationId","limit","skip"]},
    method: "get",
    pathTemplate: "/calendars/resources/{resourceType}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"resourceType","in":"path"},{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["calendars/resources.readonly"]}]
  }],
  ["create-calendar-resource", {
    name: "create-calendar-resource",
    description: `Create calendar resource by resource type`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"resourceType":{"enum":["equipments","rooms"],"type":"string","description":"Calendar Resource Type"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"name":{"type":"string"},"description":{"type":"string"},"quantity":{"type":"number","description":"Quantity of the equipment."},"outOfService":{"type":"number","description":"Quantity of the out of service equipment."},"capacity":{"type":"number","description":"Capacity of the room."},"calendarIds":{"description":"Service calendar IDs to be mapped with the resource.\n\n    One equipment can only be mapped with one service calendar.\n    \nOne room can be mapped with multiple service calendars.","maxItems":100,"type":"array","items":{"type":"string"}}},"required":["locationId","name","description","quantity","outOfService","capacity","calendarIds"],"description":"The JSON request body."}},"required":["Version","resourceType","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/resources/{resourceType}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"resourceType","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["calendars/resources.write"]}]
  }],
  ["get-event-notification", {
    name: "get-event-notification",
    description: `Get calendar notifications based on query`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string"},"isActive":{"type":"boolean"},"deleted":{"type":"boolean"},"limit":{"default":100,"type":"number","description":"Number of records to return"},"skip":{"default":0,"type":"number","description":"Number of records to skip"}},"required":["Version","calendarId"]},
    method: "get",
    pathTemplate: "/calendars/{calendarId}/notifications",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"},{"name":"isActive","in":"query"},{"name":"deleted","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars/events.readonly"]}]
  }],
  ["create-event-notification", {
    name: "create-event-notification",
    description: `Create Calendar notifications, either one or multiple. All notification settings must be for single calendar only`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string"},"requestBody":{"type":"array","items":{"type":"object","properties":{"receiverType":{"type":"string","description":"notification recipient type","enum":["contact","guest","assignedUser","emails"]},"channel":{"type":"string","description":"Notification channel","enum":["email","inApp"]},"notificationType":{"type":"string","description":"Notification type","enum":["booked","confirmation","cancellation","reminder","followup","reschedule"]},"isActive":{"type":"boolean","description":"Is the notification active","default":true},"templateId":{"type":"string","description":"Template ID for email notification. Not necessary for in-App notification"},"body":{"type":"string","description":"Body  for email notification. Not necessary for in-App notification"},"subject":{"type":"string","description":"Subject  for email notification. Not necessary for in-App notification"},"afterTime":{"description":"Specifies the time after which the follow-up notification should be sent. This is not required for other notification types.","type":"array","items":{"type":"object","properties":{"timeOffset":{"type":"number"},"unit":{"type":"string"}}}},"beforeTime":{"description":"Specifies the time before which the reminder notification should be sent. This is not required for other notification types.","type":"array","items":{"type":"object","properties":{"timeOffset":{"type":"number"},"unit":{"type":"string"}}}},"additionalEmailIds":{"description":"Additional email addresses to receive notifications.","type":"array","items":{"type":"string"}},"selectedUsers":{"description":"selected user for in-App notification","type":"array","items":{"type":"string"}},"fromAddress":{"type":"string","description":"from address for email notification"},"fromName":{"type":"string","description":"from name for email notification"}},"required":["receiverType","channel","notificationType"]},"description":"The JSON request body."}},"required":["Version","calendarId","requestBody"]},
    method: "post",
    pathTemplate: "/calendars/{calendarId}/notifications",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars/events.write"]}]
  }],
  ["find-event-notification", {
    name: "find-event-notification",
    description: `Find Event notification by notificationId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string"},"notificationId":{"type":"string"}},"required":["Version","calendarId","notificationId"]},
    method: "get",
    pathTemplate: "/calendars/{calendarId}/notifications/{notificationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"},{"name":"notificationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-event-notification", {
    name: "update-event-notification",
    description: `Update Event notification by id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string"},"notificationId":{"type":"string"},"requestBody":{"type":"object","properties":{"receiverType":{"type":"string","description":"Notification recipient type","enum":["contact","guest","assignedUser","emails"]},"additionalEmailIds":{"description":"Additional email addresses to receive notifications.","type":"array","items":{"type":"string"}},"selectedUsers":{"description":"selected user for in-App notification","type":"array","items":{"type":"string"}},"channel":{"type":"string","description":"Notification channel","enum":["email","inApp"]},"notificationType":{"type":"string","description":"Notification type","enum":["booked","confirmation","cancellation","reminder","followup","reschedule"]},"isActive":{"type":"boolean","description":"Is the notification active","default":true},"deleted":{"type":"boolean","description":"Marks the notification as deleted (soft delete)","default":false},"templateId":{"type":"string","description":"Template ID for email notification"},"body":{"type":"string","description":"Body  for email notification. Not necessary for in-App notification"},"subject":{"type":"string","description":"Subject  for email notification. Not necessary for in-App notification"},"afterTime":{"description":"Specifies the time after which the follow-up notification should be sent. This is not required for other notification types.","type":"array","items":{"type":"object","properties":{"timeOffset":{"type":"number"},"unit":{"type":"string"}}}},"beforeTime":{"description":"Specifies the time before which the reminder notification should be sent. This is not required for other notification types.","type":"array","items":{"type":"object","properties":{"timeOffset":{"type":"number"},"unit":{"type":"string"}}}},"fromAddress":{"type":"string","description":"From address for email notification"},"fromName":{"type":"string","description":"From name for email notification"}},"description":"The JSON request body."}},"required":["Version","calendarId","notificationId","requestBody"]},
    method: "put",
    pathTemplate: "/calendars/{calendarId}/notifications/{notificationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"},{"name":"notificationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-event-notification", {
    name: "delete-event-notification",
    description: `Delete notification`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"calendarId":{"type":"string"},"notificationId":{"type":"string"}},"required":["Version","calendarId","notificationId"]},
    method: "delete",
    pathTemplate: "/calendars/{calendarId}/notifications/{notificationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"calendarId","in":"path"},{"name":"notificationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-company", {
    name: "get-company",
    description: `Get Comapny`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"companyId":{"type":"string"}},"required":["Version","companyId"]},
    method: "get",
    pathTemplate: "/companies/{companyId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["companies.readonly"]}]
  }],
  ["search-contacts-advanced", {
    name: "search-contacts-advanced",
    description: `Search contacts based on combinations of advanced filters. Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-158396/6e629989abe7fad`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{},"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/search",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["get-duplicate-contact", {
    name: "get-duplicate-contact",
    description: `Get Duplicate Contact.<br/><br/>If \`Allow Duplicate Contact\` is disabled under Settings, the global unique identifier will be used for searching the contact. If the setting is enabled, first priority for search is \`email\` and the second priority will be \`phone\`.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"number":{"type":"string","description":"Phone Number - Pass in URL Encoded form. i.e +1423164516 will become `%2B1423164516`"},"email":{"type":"string","description":"Email - Pass in URL Encoded form. i.e test+abc@gmail.com will become `test%2Babc%40gmail.com`"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/contacts/search/duplicate",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"number","in":"query"},{"name":"email","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["get-all-tasks", {
    name: "get-all-tasks",
    description: `Get all Tasks`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}/tasks",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["create-task", {
    name: "create-task",
    description: `Create Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"},"dueDate":{"type":"string"},"completed":{"type":"boolean"},"assignedTo":{"type":"string"}},"required":["title","dueDate","completed"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/tasks",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["get-task", {
    name: "get-task",
    description: `Get Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"taskId":{"type":"string","description":"Task Id"}},"required":["Version","contactId","taskId"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}/tasks/{taskId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"taskId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["update-task", {
    name: "update-task",
    description: `Update Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"taskId":{"type":"string","description":"Task Id"},"requestBody":{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"},"dueDate":{"type":"string"},"completed":{"type":"boolean"},"assignedTo":{"type":"string"}},"description":"The JSON request body."}},"required":["Version","contactId","taskId","requestBody"]},
    method: "put",
    pathTemplate: "/contacts/{contactId}/tasks/{taskId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"taskId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["delete-task", {
    name: "delete-task",
    description: `Delete Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"taskId":{"type":"string","description":"Task Id"}},"required":["Version","contactId","taskId"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/tasks/{taskId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"taskId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["update-task-completed", {
    name: "update-task-completed",
    description: `Update Task Completed`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"taskId":{"type":"string","description":"Task Id"},"requestBody":{"type":"object","properties":{"completed":{"type":"boolean"}},"required":["completed"],"description":"The JSON request body."}},"required":["Version","contactId","taskId","requestBody"]},
    method: "put",
    pathTemplate: "/contacts/{contactId}/tasks/{taskId}/completed",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"taskId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["get-appointments-for-contact", {
    name: "get-appointments-for-contact",
    description: `Get Appointments for Contact`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}/appointments",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["add-tags", {
    name: "add-tags",
    description: `Add Tags`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}},"required":["tags"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/tags",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["remove-tags", {
    name: "remove-tags",
    description: `Remove Tags`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}},"required":["tags"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/tags",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["get-all-notes", {
    name: "get-all-notes",
    description: `Get All Notes`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}/notes",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["create-note", {
    name: "create-note",
    description: `Create Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"userId":{"type":"string"},"body":{"type":"string"}},"required":["body"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/notes",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["get-note", {
    name: "get-note",
    description: `Get Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"id":{"type":"string","description":"Note Id"}},"required":["Version","contactId","id"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}/notes/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["update-note", {
    name: "update-note",
    description: `Update Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"id":{"type":"string","description":"Note Id"},"requestBody":{"type":"object","properties":{"userId":{"type":"string"},"body":{"type":"string"}},"required":["body"],"description":"The JSON request body."}},"required":["Version","contactId","id","requestBody"]},
    method: "put",
    pathTemplate: "/contacts/{contactId}/notes/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["delete-note", {
    name: "delete-note",
    description: `Delete Note`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"id":{"type":"string","description":"Note Id"}},"required":["Version","contactId","id"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/notes/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["create-association", {
    name: "create-association",
    description: `Allows you to update tags to multiple contacts at once, you can add or remove tags from the contacts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"type":{"type":"string","enum":["add","remove"],"description":"Tags operation type"},"requestBody":{"type":"object","properties":{"contacts":{"description":"list of contact ids to be processed","type":"array","items":{"type":"string"}},"tags":{"description":"list of tags to be added or removed","type":"array","items":{"type":"string"}},"locationId":{"type":"string","description":"location id from where the bulk request is executed"},"removeAllTags":{"type":"boolean","description":"Option to implement remove all tags. if true, all tags will be removed from the contacts. Can only be used with remove type."}},"required":["contacts","tags","locationId"],"description":"The JSON request body."}},"required":["Version","type","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/bulk/tags/update/{type}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"type","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["add-remove-contact-from-business", {
    name: "add-remove-contact-from-business",
    description: `Add/Remove Contacts From Business . Passing a \`null\` businessId will remove the businessId from the contacts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"ids":{"type":"array","items":{"type":"string","maxLength":50}},"businessId":{"type":["string","null"]}},"required":["locationId","ids","businessId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/bulk/business",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["get-contact", {
    name: "get-contact",
    description: `Get Contact`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "get",
    pathTemplate: "/contacts/{contactId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["update-contact", {
    name: "update-contact",
    description: `Please find the list of acceptable values for the \`country\` field  <a href="https://highlevel.stoplight.io/docs/integrations/ZG9jOjI4MzUzNDIy-country-list" target="_blank">here</a>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"firstName":{"type":["string","null"]},"lastName":{"type":["string","null"]},"name":{"type":["string","null"]},"email":{"type":["string","null"]},"phone":{"type":["string","null"]},"address1":{"type":["string","null"]},"city":{"type":["string","null"]},"state":{"type":["string","null"]},"postalCode":{"type":"string"},"website":{"type":["string","null"]},"timezone":{"type":["string","null"]},"dnd":{"type":"boolean"},"dndSettings":{"type":"object","properties":{"Call":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"Email":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"SMS":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"WhatsApp":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"GMB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"FB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]}}},"inboundDndSettings":{"type":"object","properties":{"all":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive"]},"message":{"type":"string"}},"required":["status"]}}},"tags":{"description":"This field will overwrite all current tags associated with the contact. To update a tags, it is recommended to use the Add Tag or Remove Tag API instead.","type":"array","items":{"type":"string"}},"customFields":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100.5}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":{"f31175d4-2b06-4fc6-b7bc-74cd814c68cb":{"meta":{"fieldname":"1HeGizb13P0odwgOgKSs","originalname":"IMG_20231215_164412935.jpg","encoding":"7bit","mimetype":"image/jpeg","size":1786611,"uuid":"f31175d4-2b06-4fc6-b7bc-74cd814c68cb"},"url":"https://services.leadconnectorhq.com/documents/download/w2M9qTZ0ZJz8rGt02jdJ","documentId":"w2M9qTZ0ZJz8rGt02jdJ"}}}},"required":["id"]}]}},"source":{"type":["string","null"]},"country":{"type":"string"},"assignedTo":{"type":["string","null"],"description":"User's Id"}},"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "put",
    pathTemplate: "/contacts/{contactId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["delete-contact", {
    name: "delete-contact",
    description: `Delete Contact`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["upsert-contact", {
    name: "upsert-contact",
    description: `Please find the list of acceptable values for the \`country\` field  <a href="https://highlevel.stoplight.io/docs/integrations/ZG9jOjI4MzUzNDIy-country-list" target="_blank">here</a><br/><br/>The Upsert API will adhere to the configuration defined under the “Allow Duplicate Contact” setting at the Location level. If the setting is configured to check both Email and Phone, the API will attempt to identify an existing contact based on the priority sequence specified in the setting, and will create or update the contact accordingly.<br/><br/>If two separate contacts already exist—one with the same email and another with the same phone—and an upsert request includes both the email and phone, the API will update the contact that matches the first field in the configured sequence, and ignore the second field to prevent duplication.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"firstName":{"type":["string","null"]},"lastName":{"type":["string","null"]},"name":{"type":["string","null"]},"email":{"type":["string","null"]},"locationId":{"type":"string"},"gender":{"type":"string"},"phone":{"type":["string","null"]},"address1":{"type":["string","null"]},"city":{"type":["string","null"]},"state":{"type":["string","null"]},"postalCode":{"type":"string"},"website":{"type":["string","null"]},"timezone":{"type":["string","null"]},"dnd":{"type":"boolean"},"dndSettings":{"type":"object","properties":{"Call":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"Email":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"SMS":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"WhatsApp":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"GMB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"FB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]}}},"inboundDndSettings":{"type":"object","properties":{"all":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive"]},"message":{"type":"string"}},"required":["status"]}}},"tags":{"description":"This field will overwrite all current tags associated with the contact. To update a tags, it is recommended to use the Add Tag or Remove Tag API instead.","type":"array","items":{"type":"string"}},"customFields":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100.5}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":{"f31175d4-2b06-4fc6-b7bc-74cd814c68cb":{"meta":{"fieldname":"1HeGizb13P0odwgOgKSs","originalname":"IMG_20231215_164412935.jpg","encoding":"7bit","mimetype":"image/jpeg","size":1786611,"uuid":"f31175d4-2b06-4fc6-b7bc-74cd814c68cb"},"url":"https://services.leadconnectorhq.com/documents/download/w2M9qTZ0ZJz8rGt02jdJ","documentId":"w2M9qTZ0ZJz8rGt02jdJ"}}}},"required":["id"]}]}},"source":{"type":"string"},"country":{"type":"string"},"companyName":{"type":["string","null"]},"assignedTo":{"type":"string","description":"User's Id"}},"required":["locationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/upsert",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["get-contacts-by-businessId", {
    name: "get-contacts-by-businessId",
    description: `Get Contacts By BusinessId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"businessId":{"type":"string"},"limit":{"default":"25","type":"string"},"locationId":{"type":"string"},"skip":{"default":"0","type":"string"},"query":{"type":"string"}},"required":["Version","businessId","locationId"]},
    method: "get",
    pathTemplate: "/contacts/business/{businessId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"businessId","in":"path"},{"name":"limit","in":"query"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"query","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["add-followers-contact", {
    name: "add-followers-contact",
    description: `Add Followers`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"followers":{"type":"array","items":{"type":"string"}}},"required":["followers"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/followers",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["remove-followers-contact", {
    name: "remove-followers-contact",
    description: `Remove Followers`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"requestBody":{"type":"object","properties":{"followers":{"type":"array","items":{"type":"string"}}},"required":["followers"],"description":"The JSON request body."}},"required":["Version","contactId","requestBody"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/followers",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["add-contact-to-campaign", {
    name: "add-contact-to-campaign",
    description: `Add contact to Campaign`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"campaignId":{"type":"string","description":"Campaigns Id"},"requestBody":{"type":"object","properties":{},"description":"The JSON request body."}},"required":["Version","contactId","campaignId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/campaigns/{campaignId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"campaignId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["remove-contact-from-campaign", {
    name: "remove-contact-from-campaign",
    description: `Remove Contact From Campaign`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"campaignId":{"type":"string","description":"Campaigns Id"}},"required":["Version","contactId","campaignId"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/campaigns/{campaignId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"campaignId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["remove-contact-from-every-campaign", {
    name: "remove-contact-from-every-campaign",
    description: `Remove Contact From Every Campaign`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"}},"required":["Version","contactId"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/campaigns/removeAll",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["add-contact-to-workflow", {
    name: "add-contact-to-workflow",
    description: `Add Contact to Workflow`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"workflowId":{"type":"string","description":"Workflow Id"},"requestBody":{"type":"object","properties":{"eventStartTime":{"type":"string"}},"description":"The JSON request body."}},"required":["Version","contactId","workflowId","requestBody"]},
    method: "post",
    pathTemplate: "/contacts/{contactId}/workflow/{workflowId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"workflowId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["delete-contact-from-workflow", {
    name: "delete-contact-from-workflow",
    description: `Delete Contact from Workflow`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact Id"},"workflowId":{"type":"string","description":"Workflow Id"},"requestBody":{"type":"object","properties":{"eventStartTime":{"type":"string"}},"description":"The JSON request body."}},"required":["Version","contactId","workflowId","requestBody"]},
    method: "delete",
    pathTemplate: "/contacts/{contactId}/workflow/{workflowId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"path"},{"name":"workflowId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["search-conversation", {
    name: "search-conversation",
    description: `Returns a list of all conversations matching the search criteria along with the sort and filter options selected.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"contactId":{"type":"string","description":"Contact Id"},"assignedTo":{"type":"string","description":"User IDs that conversations are assigned to. Multiple IDs can be provided as comma-separated values. Use \"unassigned\" to fetch conversations not assigned to any user."},"followers":{"type":"string","description":"User IDs of followers to filter conversations by. Multiple IDs can be provided as comma-separated values."},"mentions":{"type":"string","description":"User Id of the mention. Multiple values are comma separated."},"query":{"type":"string","description":"Search paramater as a string"},"sort":{"enum":["asc","desc"],"type":"string","description":"Sort paramater - asc or desc"},"startAfterDate":{"type":"string","anyOf":[{"type":"object","properties":{"startAfterDate":{"type":"number","description":"Search to begin after the specified date - should contain the sort value of the last document","example":1600854}}},{"type":"object","properties":{"startAfterDate":{"description":"Search to begin after the specified date - should contain the sort value of the last document","example":[1600854,1600851],"type":"array","items":{"type":"string"}}}}],"description":"Search to begin after the specified date - should contain the sort value of the last document"},"id":{"type":"string","description":"Id of the conversation"},"limit":{"type":"number","description":"Limit of conversations - Default is 20"},"lastMessageType":{"enum":["TYPE_CALL","TYPE_SMS","TYPE_EMAIL","TYPE_SMS_REVIEW_REQUEST","TYPE_WEBCHAT","TYPE_SMS_NO_SHOW_REQUEST","TYPE_CAMPAIGN_SMS","TYPE_CAMPAIGN_CALL","TYPE_CAMPAIGN_EMAIL","TYPE_CAMPAIGN_VOICEMAIL","TYPE_FACEBOOK","TYPE_CAMPAIGN_FACEBOOK","TYPE_CAMPAIGN_MANUAL_CALL","TYPE_CAMPAIGN_MANUAL_SMS","TYPE_GMB","TYPE_CAMPAIGN_GMB","TYPE_REVIEW","TYPE_INSTAGRAM","TYPE_WHATSAPP","TYPE_CUSTOM_SMS","TYPE_CUSTOM_EMAIL","TYPE_CUSTOM_PROVIDER_SMS","TYPE_CUSTOM_PROVIDER_EMAIL","TYPE_IVR_CALL","TYPE_ACTIVITY_CONTACT","TYPE_ACTIVITY_INVOICE","TYPE_ACTIVITY_PAYMENT","TYPE_ACTIVITY_OPPORTUNITY","TYPE_LIVE_CHAT","TYPE_LIVE_CHAT_INFO_MESSAGE","TYPE_ACTIVITY_APPOINTMENT","TYPE_FACEBOOK_COMMENT","TYPE_INSTAGRAM_COMMENT","TYPE_CUSTOM_CALL","TYPE_INTERNAL_COMMENT","TYPE_ACTIVITY_EMPLOYEE_ACTION_LOG"],"type":"string","description":"Type of the last message in the conversation as a string"},"lastMessageAction":{"enum":["automated","manual"],"type":"string","description":"Action of the last outbound message in the conversation as string."},"lastMessageDirection":{"enum":["inbound","outbound"],"type":"string","description":"Direction of the last message in the conversation as string."},"status":{"enum":["all","read","unread","starred","recents"],"type":"string","description":"The status of the conversation to be filtered - all, read, unread, starred "},"sortBy":{"enum":["last_manual_message_date","last_message_date","score_profile"],"type":"string","description":"The sorting of the conversation to be filtered as - manual messages or all messages"},"sortScoreProfile":{"type":"string","description":"Id of score profile on which sortBy.ScoreProfile should sort on"},"scoreProfile":{"type":"string","description":"Id of score profile on which conversations should get filtered out, works with scoreProfileMin & scoreProfileMax"},"scoreProfileMin":{"type":"number","description":"Minimum value for score"},"scoreProfileMax":{"type":"number","description":"Maximum value for score"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/conversations/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"contactId","in":"query"},{"name":"assignedTo","in":"query"},{"name":"followers","in":"query"},{"name":"mentions","in":"query"},{"name":"query","in":"query"},{"name":"sort","in":"query"},{"name":"startAfterDate","in":"query"},{"name":"id","in":"query"},{"name":"limit","in":"query"},{"name":"lastMessageType","in":"query"},{"name":"lastMessageAction","in":"query"},{"name":"lastMessageDirection","in":"query"},{"name":"status","in":"query"},{"name":"sortBy","in":"query"},{"name":"sortScoreProfile","in":"query"},{"name":"scoreProfile","in":"query"},{"name":"scoreProfileMin","in":"query"},{"name":"scoreProfileMax","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations.readonly"]}]
  }],
  ["get-conversation", {
    name: "get-conversation",
    description: `Get the conversation details based on the conversation ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"conversationId":{"type":"string","description":"Conversation ID as string"}},"required":["Version","conversationId"]},
    method: "get",
    pathTemplate: "/conversations/{conversationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"conversationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations.readonly"]}]
  }],
  ["update-conversation", {
    name: "update-conversation",
    description: `Update the conversation details based on the conversation ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"conversationId":{"type":"string","description":"Conversation ID as string"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location ID as string"},"unreadCount":{"type":"number","description":"Count of unread messages in the conversation"},"starred":{"type":"boolean","description":"Starred status of the conversation."},"feedback":{"type":"object"}},"required":["locationId"],"description":"The JSON request body."}},"required":["Version","conversationId","requestBody"]},
    method: "put",
    pathTemplate: "/conversations/{conversationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"conversationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations.write"]}]
  }],
  ["delete-conversation", {
    name: "delete-conversation",
    description: `Delete the conversation details based on the conversation ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"conversationId":{"type":"string","description":"Conversation ID as string"}},"required":["Version","conversationId"]},
    method: "delete",
    pathTemplate: "/conversations/{conversationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"conversationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations.write"]}]
  }],
  ["get-email-by-id", {
    name: "get-email-by-id",
    description: `Get email by Id`,
    inputSchema: {"type":"object","properties":{"id":{"type":"string"}},"required":["id"]},
    method: "get",
    pathTemplate: "/conversations/messages/email/{id}",
    executionParameters: [{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["cancel-scheduled-email-message", {
    name: "cancel-scheduled-email-message",
    description: `Post the messageId for the API to delete a scheduled email message. <br />`,
    inputSchema: {"type":"object","properties":{"emailMessageId":{"type":"string","description":"Email Message Id"}},"required":["emailMessageId"]},
    method: "delete",
    pathTemplate: "/conversations/messages/email/{emailMessageId}/schedule",
    executionParameters: [{"name":"emailMessageId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["get-message", {
    name: "get-message",
    description: `Get message by message id.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"id":{"type":"string"}},"required":["Version","id"]},
    method: "get",
    pathTemplate: "/conversations/messages/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations/message.readonly"]}]
  }],
  ["get-messages", {
    name: "get-messages",
    description: `Get messages by conversation id.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"conversationId":{"type":"string","description":"Conversation ID as string"},"lastMessageId":{"type":"string","description":"Message ID of the last message in the list as a string"},"limit":{"type":"number","description":"Number of messages to be fetched from the conversation. Default limit is 20"},"type":{"enum":["TYPE_CALL","TYPE_SMS","TYPE_EMAIL","TYPE_FACEBOOK","TYPE_GMB","TYPE_INSTAGRAM","TYPE_WHATSAPP","TYPE_ACTIVITY_APPOINTMENT","TYPE_ACTIVITY_CONTACT","TYPE_ACTIVITY_INVOICE","TYPE_ACTIVITY_PAYMENT","TYPE_ACTIVITY_OPPORTUNITY","TYPE_LIVE_CHAT","TYPE_INTERNAL_COMMENTS","TYPE_ACTIVITY_EMPLOYEE_ACTION_LOG"],"type":"string","description":"Types of message to fetched separated with comma"}},"required":["Version","conversationId"]},
    method: "get",
    pathTemplate: "/conversations/{conversationId}/messages",
    executionParameters: [{"name":"Version","in":"header"},{"name":"conversationId","in":"path"},{"name":"lastMessageId","in":"query"},{"name":"limit","in":"query"},{"name":"type","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations/message.readonly"]}]
  }],
  ["send-a-new-message", {
    name: "send-a-new-message",
    description: `Post the necessary fields for the API to send a new message.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"type":{"type":"string","enum":["SMS","Email","WhatsApp","IG","FB","Custom","Live_Chat"],"description":"Type of message being sent"},"contactId":{"type":"string","description":"ID of the contact receiving the message"},"appointmentId":{"type":"string","description":"ID of the associated appointment"},"attachments":{"description":"Array of attachment URLs","type":"array","items":{"type":"string"}},"emailFrom":{"type":"string","description":"Email address to send from"},"emailCc":{"description":"Array of CC email addresses","type":"array","items":{"type":"string"}},"emailBcc":{"description":"Array of BCC email addresses","type":"array","items":{"type":"string"}},"html":{"type":"string","description":"HTML content of the message"},"message":{"type":"string","description":"Text content of the message"},"subject":{"type":"string","description":"Subject line for email messages"},"replyMessageId":{"type":"string","description":"ID of message being replied to"},"templateId":{"type":"string","description":"ID of message template"},"threadId":{"type":"string","description":"ID of message thread. For email messages, this is the message ID that contains multiple email messages in the thread"},"scheduledTimestamp":{"type":"number","description":"UTC Timestamp (in seconds) at which the message should be scheduled"},"conversationProviderId":{"type":"string","description":"ID of conversation provider"},"emailTo":{"type":"string","description":"Email address to send to, if different from contact's primary email. This should be a valid email address associated with the contact."},"emailReplyMode":{"type":"string","enum":["reply","reply_all"],"description":"Mode for email replies"},"fromNumber":{"type":"string","description":"Phone number used as the sender number for outbound messages"},"toNumber":{"type":"string","description":"Recipient phone number for outbound messages"}},"required":["type","contactId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations/messages",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["add-an-inbound-message", {
    name: "add-an-inbound-message",
    description: `Post the necessary fields for the API to add a new inbound message. <br />`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"type":{"type":"string","description":"Message Type","enum":["SMS","Email","WhatsApp","GMB","IG","FB","Custom","WebChat","Live_Chat","Call"]},"attachments":{"description":"Array of attachments","type":"array","items":{"type":"string"}},"message":{"type":"string","description":"Message Body"},"conversationId":{"type":"string","description":"Conversation Id"},"conversationProviderId":{"type":"string","description":"Conversation Provider Id"},"html":{"type":"string","description":"HTML Body of Email"},"subject":{"type":"string","description":"Subject of the Email"},"emailFrom":{"type":"string","description":"Email address to send from. This field is associated with the contact record and cannot be dynamically changed."},"emailTo":{"type":"string","description":"Recipient email address. This field is associated with the contact record and cannot be dynamically changed."},"emailCc":{"description":"List of email address to CC","type":"array","items":{"type":"string"}},"emailBcc":{"description":"List of email address to BCC","type":"array","items":{"type":"string"}},"emailMessageId":{"type":"string","description":"Send the email message id for which this email should be threaded. This is for replying to a specific email"},"altId":{"type":"string","description":"external mail provider's message id"},"direction":{"type":"object","description":"Message direction, if required can be set manually, default is outbound","default":"outbound"},"date":{"format":"date-time","type":"string","description":"Date of the inbound message"},"call":{"description":"Phone call dialer and receiver information","allOf":[{"type":"object","properties":{"to":{"type":"string","description":"Phone number of the receiver","example":"+15037081210"},"from":{"type":"string","description":"Phone number of the dialer","example":"+15037081210"},"status":{"type":"string","description":"Call status","example":"completed","enum":["pending","completed","answered","busy","no-answer","failed","canceled","voicemail"]}}}]}},"required":["type","conversationId","conversationProviderId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations/messages/inbound",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["add-an-outbound-message", {
    name: "add-an-outbound-message",
    description: `Post the necessary fields for the API to add a new outbound call.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"type":{"type":"string","description":"Message Type","enum":["Call"]},"attachments":{"description":"Array of attachments","type":"array","items":{"type":"string"}},"conversationId":{"type":"string","description":"Conversation Id"},"conversationProviderId":{"type":"string","description":"Conversation Provider Id"},"altId":{"type":"string","description":"external mail provider's message id"},"date":{"format":"date-time","type":"string","description":"Date of the outbound message"},"call":{"description":"Phone call dialer and receiver information","allOf":[{"type":"object","properties":{"to":{"type":"string","description":"Phone number of the receiver","example":"+15037081210"},"from":{"type":"string","description":"Phone number of the dialer","example":"+15037081210"},"status":{"type":"string","description":"Call status","example":"completed","enum":["pending","completed","answered","busy","no-answer","failed","canceled","voicemail"]}}}]}},"required":["type","conversationId","conversationProviderId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations/messages/outbound",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["cancel-scheduled-message", {
    name: "cancel-scheduled-message",
    description: `Post the messageId for the API to delete a scheduled message. <br />`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"messageId":{"type":"string","description":"Message Id"}},"required":["Version","messageId"]},
    method: "delete",
    pathTemplate: "/conversations/messages/{messageId}/schedule",
    executionParameters: [{"name":"Version","in":"header"},{"name":"messageId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["upload-file-attachments", {
    name: "upload-file-attachments",
    description: `Post the necessary fields for the API to upload files. The files need to be a buffer with the key "fileAttachment". <br /><br /> The allowed file types are: <br/> <ul><li>JPG</li><li>JPEG</li><li>PNG</li><li>MP4</li><li>MPEG</li><li>ZIP</li><li>RAR</li><li>PDF</li><li>DOC</li><li>DOCX</li><li>TXT</li><li>MP3</li><li>WAV</li></ul> <br /><br /> The API will return an object with the URLs`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"string","description":"Request body (content type: multipart/form-data)"}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations/messages/upload",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "multipart/form-data",
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["update-message-status", {
    name: "update-message-status",
    description: `Post the necessary fields for the API to update message status.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"messageId":{"type":"string","description":"Message Id"},"requestBody":{"type":"object","properties":{"status":{"type":"string","description":"Message status","enum":["delivered","failed","pending","read"]},"error":{"description":"Error object from the conversation provider","allOf":[{"type":"object","properties":{"code":{"type":"string","description":"Error Code","example":"1"},"type":{"type":"string","description":"Error Type","example":"saas"},"message":{"type":"string","description":"Error Message","example":"There was an error from the provider"}},"required":["code","type","message"]}]},"emailMessageId":{"type":"string","description":"Email message Id"},"recipients":{"description":"Email delivery status for additional email recipients.","type":"array","items":{"type":"string"}}},"required":["status"],"description":"The JSON request body."}},"required":["Version","messageId","requestBody"]},
    method: "put",
    pathTemplate: "/conversations/messages/{messageId}/status",
    executionParameters: [{"name":"Version","in":"header"},{"name":"messageId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations/message.write"]}]
  }],
  ["get-message-recording", {
    name: "get-message-recording",
    description: `Get the recording for a message by passing the message id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID as string"},"messageId":{"type":"string","description":"Message ID as string"}},"required":["Version","locationId","messageId"]},
    method: "get",
    pathTemplate: "/conversations/messages/{messageId}/locations/{locationId}/recording",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"messageId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]},{"Location-Access":[]}]
  }],
  ["get-message-transcription", {
    name: "get-message-transcription",
    description: `Get the recording transcription for a message by passing the message id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID as string"},"messageId":{"type":"string","description":"Message ID as string"}},"required":["Version","locationId","messageId"]},
    method: "get",
    pathTemplate: "/conversations/locations/{locationId}/messages/{messageId}/transcription",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"messageId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]},{"Location-Access":[]}]
  }],
  ["download-message-transcription", {
    name: "download-message-transcription",
    description: `Download the recording transcription for a message by passing the message id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID as string"},"messageId":{"type":"string","description":"Message ID as string"}},"required":["Version","locationId","messageId"]},
    method: "get",
    pathTemplate: "/conversations/locations/{locationId}/messages/{messageId}/transcription/download",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"messageId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]},{"Location-Access":[]}]
  }],
  ["live-chat-agent-typing", {
    name: "live-chat-agent-typing",
    description: `Agent/AI-Bot will call this when they are typing a message in live chat message`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location Id"},"isTyping":{"type":"string","description":"Typing status"},"visitorId":{"type":"string","description":"visitorId is the Unique ID assigned to each Live chat visitor. visitorId will be added soon in <a href=\"https://highlevel.stoplight.io/docs/integrations/00c5ff21f0030-get-contact\" target=\"_blank\">GET Contact API</a>"},"conversationId":{"type":"string","description":"Conversation Id"}},"required":["locationId","isTyping","visitorId","conversationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations/providers/live-chat/typing",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["conversations/livechat.write"]}]
  }],
  ["import-courses", {
    name: "import-courses",
    description: `Import Courses through public channels`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"userId":{"type":"string"},"products":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"description":{"type":"string"},"imageUrl":{"type":"string"},"categories":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"visibility":{"type":"string","enum":["published","draft"]},"thumbnailUrl":{"type":"string"},"posts":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"visibility":{"type":"string","enum":["published","draft"]},"thumbnailUrl":{"type":"string"},"contentType":{"type":"string","enum":["video","assignment","quiz"]},"description":{"type":"string"},"bucketVideoUrl":{"type":"string"},"postMaterials":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"type":{"type":"string","enum":["pdf","image","docx","pptx","xlsx","html","dotx","epub","webp","gdoc","mp3","doc","txt","zip","ppt","key","htm","xls","odp","odt","rtf","m4a","ods","mp4","ai","avi","mov","wmv","mkv","wav","flac","ogg","png","jpeg","jpg","gif","bmp","tiff","svg","odg","sxw","sxc","sxi","rar","7z","json","xml","csv","md","obj","stl","woff","ttf"]},"url":{"type":"string"}},"required":["title","type","url"]}}},"required":["title","visibility","contentType","description"]}},"subCategories":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"visibility":{"type":"string","enum":["published","draft"]},"thumbnailUrl":{"type":"string"},"posts":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"visibility":{"type":"string","enum":["published","draft"]},"thumbnailUrl":{"type":"string"},"contentType":{"type":"string","enum":["video","assignment","quiz"]},"description":{"type":"string"},"bucketVideoUrl":{"type":"string"},"postMaterials":{"type":"array","items":{"type":"object","properties":{"title":{"type":"string"},"type":{"type":"string","enum":["pdf","image","docx","pptx","xlsx","html","dotx","epub","webp","gdoc","mp3","doc","txt","zip","ppt","key","htm","xls","odp","odt","rtf","m4a","ods","mp4","ai","avi","mov","wmv","mkv","wav","flac","ogg","png","jpeg","jpg","gif","bmp","tiff","svg","odg","sxw","sxc","sxi","rar","7z","json","xml","csv","md","obj","stl","woff","ttf"]},"url":{"type":"string"}},"required":["title","type","url"]}}},"required":["title","visibility","contentType","description"]}}},"required":["title","visibility"]}}},"required":["title","visibility"]}},"instructorDetails":{"type":"object","properties":{"name":{"type":"string"},"description":{"type":"string"}},"required":["name","description"]}},"required":["title","description","categories"]}}},"required":["locationId","products"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/courses/courses-exporter/public/import",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-custom-field-by-id", {
    name: "get-custom-field-by-id",
    description: `<div>
                  <p> Get Custom Field / Folder By Id.</p> 
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                        Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
                        </strong>
                      </span>
                  </div>
                </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"}},"required":["Version","id"]},
    method: "get",
    pathTemplate: "/custom-fields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customFields.readonly"]}]
  }],
  ["update-custom-field", {
    name: "update-custom-field",
    description: `<div>
    <p> Update Custom Field By Id </p> 
    <div>
      <span style= "display: inline-block;
                  width: 25px; height: 25px;
                  background-color: yellow;
                  color: black;
                  font-weight: bold;
                  font-size: 24px;
                  text-align: center;
                  line-height: 22px;
                  border: 2px solid black;
                  border-radius: 10%;
                  margin-right: 10px;">
                  !
        </span>
        <span>
          <strong>
          Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
          </strong>
        </span>
    </div>
  </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"placeholder":{"type":"string"},"acceptedFormat":{"type":"array","items":{"type":"string"}},"isMultipleFile":{"type":"boolean"},"maxNumberOfFiles":{"type":"number"},"textBoxListOptions":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}},{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}}]}},"position":{"type":"number","default":0},"model":{"type":"string","description":"Model of the custom field you want to update","enum":["contact","opportunity"]}},"required":["name"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "put",
    pathTemplate: "/custom-fields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["delete-custom-field", {
    name: "delete-custom-field",
    description: `<div>
    <p> Delete Custom Field By Id </p> 
    <div>
      <span style= "display: inline-block;
                  width: 25px; height: 25px;
                  background-color: yellow;
                  color: black;
                  font-weight: bold;
                  font-size: 24px;
                  text-align: center;
                  line-height: 22px;
                  border: 2px solid black;
                  border-radius: 10%;
                  margin-right: 10px;">
                  !
        </span>
        <span>
          <strong>
          Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
          </strong>
        </span>
    </div>
  </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"}},"required":["Version","id"]},
    method: "delete",
    pathTemplate: "/custom-fields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["get-custom-fields-by-object-key", {
    name: "get-custom-fields-by-object-key",
    description: `<div>
                  <p> Get Custom Fields By Object Key</p> 
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                        Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
                        </strong>
                      </span>
                  </div>
                </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"objectKey":{"type":"string","description":"key of the Object. Must include \"custom_objects.\" prefix for custom objects. Available on the Custom Objects details Page under settings"},"locationId":{"type":"string"}},"required":["Version","objectKey","locationId"]},
    method: "get",
    pathTemplate: "/custom-fields/object-key/{objectKey}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"objectKey","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customFields.readonly"]}]
  }],
  ["create-custom-field-folder", {
    name: "create-custom-field-folder",
    description: `<div>
    <p> Create Custom Field Folder </p> 
    <div>
      <span style= "display: inline-block;
                  width: 25px; height: 25px;
                  background-color: yellow;
                  color: black;
                  font-weight: bold;
                  font-size: 24px;
                  text-align: center;
                  line-height: 22px;
                  border: 2px solid black;
                  border-radius: 10%;
                  margin-right: 10px;">
                  !
        </span>
        <span>
          <strong>
          Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
          </strong>
        </span>
    </div>
  </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"objectKey":{"type":"string","description":"The key for your custom object. This key uniquely identifies the custom object. Example: \"custom_object.pet\" for a custom object related to pets."},"name":{"type":"string","description":"Field name"},"locationId":{"type":"string","description":"Location Id"}},"required":["objectKey","name","locationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/custom-fields/folder",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["update-custom-field-folder", {
    name: "update-custom-field-folder",
    description: `<div>
    <p> Create Custom Field Folder </p> 
    <div>
      <span style= "display: inline-block;
                  width: 25px; height: 25px;
                  background-color: yellow;
                  color: black;
                  font-weight: bold;
                  font-size: 24px;
                  text-align: center;
                  line-height: 22px;
                  border: 2px solid black;
                  border-radius: 10%;
                  margin-right: 10px;">
                  !
        </span>
        <span>
          <strong>
          Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
          </strong>
        </span>
    </div>
  </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"Field name"},"locationId":{"type":"string","description":"Location Id"}},"required":["name","locationId"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "put",
    pathTemplate: "/custom-fields/folder/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["delete-custom-field-folder", {
    name: "delete-custom-field-folder",
    description: `<div>
    <p> Create Custom Field Folder </p> 
    <div>
      <span style= "display: inline-block;
                  width: 25px; height: 25px;
                  background-color: yellow;
                  color: black;
                  font-weight: bold;
                  font-size: 24px;
                  text-align: center;
                  line-height: 22px;
                  border: 2px solid black;
                  border-radius: 10%;
                  margin-right: 10px;">
                  !
        </span>
        <span>
          <strong>
          Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
          </strong>
        </span>
    </div>
  </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","id","locationId"]},
    method: "delete",
    pathTemplate: "/custom-fields/folder/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["get-custom-menu-by-id", {
    name: "get-custom-menu-by-id",
    description: `Fetches a single custom menus based on id. This endpoint allows clients to retrieve custom menu configurations, which may include menu items, categories, and associated metadata`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"customMenuId":{"type":"string","description":"Unique identifier of the custom menu"}},"required":["Version","customMenuId"]},
    method: "get",
    pathTemplate: "/custom-menus/{customMenuId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"customMenuId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["custom-menu-link.readonly"]}]
  }],
  ["update-custom-menu", {
    name: "update-custom-menu",
    description: `Updates an existing custom menu for a given company. Requires authentication and proper permissions.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"customMenuId":{"type":"string","description":"ID of the custom menu to update"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title of the custom menu"},"url":{"type":"string","description":"URL of the custom menu"},"icon":{"description":"Icon information for the custom menu","allOf":[{"type":"object","properties":{"name":{"type":"string","description":"Name of the icon","example":"yin-yang"},"fontFamily":{"type":"string","description":"Font family of the icon","enum":["fab","fas","far"]}}}]},"showOnCompany":{"type":"boolean","description":"Whether the menu must be displayed on the agency's level","default":true},"showOnLocation":{"type":"boolean","description":"Whether the menu must be displayed for sub-accounts level","default":true},"showToAllLocations":{"type":"boolean","description":"Whether the menu must be displayed to all sub-accounts","default":true},"openMode":{"type":"string","description":"Mode for opening the menu link","enum":["iframe","new_tab","current_tab"]},"locations":{"description":"List of sub-account IDs where the menu should be shown. This list is applicable only when showOnLocation is true and showToAllLocations is false","type":"array","items":{"type":"string"}},"userRole":{"type":"string","description":"Which user-roles should the menu be accessible to?","enum":["all","admin","user"]},"allowCamera":{"type":"boolean","description":"Whether to allow camera access (only for iframe mode)"},"allowMicrophone":{"type":"boolean","description":"Whether to allow microphone access (only for iframe mode)"}},"description":"The JSON request body."}},"required":["Version","customMenuId","requestBody"]},
    method: "put",
    pathTemplate: "/custom-menus/{customMenuId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"customMenuId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["custom-menu-link.write"]}]
  }],
  ["delete-custom-menu", {
    name: "delete-custom-menu",
    description: `Removes a specific custom menu from the system. This operation requires authentication and proper permissions. The custom menu is identified by its unique ID, and the operation is performed within the context of a specific company.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"customMenuId":{"type":"string","description":"ID of the custom menu to delete"}},"required":["Version","customMenuId"]},
    method: "delete",
    pathTemplate: "/custom-menus/{customMenuId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"customMenuId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["custom-menu-link.write"]}]
  }],
  ["verify-email", {
    name: "verify-email",
    description: `Verify Email`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id, The email verification charges will be deducted from this location (if rebilling is enabled) / company wallet"},"requestBody":{"type":"object","properties":{"type":{"type":"string","enum":["email","contact"],"description":"Email Verification type"},"verify":{"type":"string","description":"Email Verification recepient (email address / contactId)"}},"required":["type","verify"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/email/verify",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["fetch-campaigns", {
    name: "fetch-campaigns",
    description: `Get Campaigns`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string","description":"Location ID to fetch campaigns from"},"limit":{"type":"number","description":"Maximum number of campaigns to return. Defaults to 10, maximum is 100"},"offset":{"type":"number","description":"Number of campaigns to skip for pagination"},"status":{"default":"active","enum":["active","pause","complete","cancelled","retry","draft","resend-scheduled"],"type":"string","description":"Filter by schedule status"},"emailStatus":{"default":"complete","enum":["all","not-started","paused","cancelled","processing","resumed","next-drip","complete","success","error","waiting","queued","queueing","reading","scheduled"],"type":"string","description":"Filter by email delivery status"},"name":{"type":"string","description":"Filter campaigns by name"},"parentId":{"type":"string","description":"Filter campaigns by parent folder ID"},"limitedFields":{"type":"boolean","default":false,"description":"When true, returns only essential campaign fields like id, templateDataDownloadUrl, updatedAt, type, templateType, templateId, downloadUrl and isPlainText. When false, returns complete campaign data including meta information, bulkRequestStatusInfo, ABTestInfo, resendScheduleInfo and all other campaign properties"},"archived":{"type":"boolean","description":"Filter archived campaigns"},"campaignsOnly":{"type":"boolean","default":false,"description":"Return only campaigns, excluding folders"},"showStats":{"type":"boolean","default":true,"description":"When true, returns campaign statistics including delivered count, opened count, clicked count and revenue if available for the campaign. When false, returns campaign data without statistics."},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["locationId","Version"]},
    method: "get",
    pathTemplate: "/emails/schedule",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"status","in":"query"},{"name":"emailStatus","in":"query"},{"name":"name","in":"query"},{"name":"parentId","in":"query"},{"name":"limitedFields","in":"query"},{"name":"archived","in":"query"},{"name":"campaignsOnly","in":"query"},{"name":"showStats","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["emails/schedule.readonly"]}]
  }],
  ["fetch-template", {
    name: "fetch-template",
    description: `Fetch email templates by location id`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string"},"limit":{"default":10,"type":"string"},"offset":{"default":0,"type":"string"},"search":{"default":"","type":"string"},"sortByDate":{"default":"desc","type":"string"},"archived":{"default":false,"type":"string"},"builderVersion":{"default":"2","enum":["1","2"],"type":"string"},"name":{"default":"","type":"string"},"parentId":{"default":"","type":"string"},"originId":{"type":"string"},"templatesOnly":{"default":false,"type":"string"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["locationId","Version"]},
    method: "get",
    pathTemplate: "/emails/builder",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"search","in":"query"},{"name":"sortByDate","in":"query"},{"name":"archived","in":"query"},{"name":"builderVersion","in":"query"},{"name":"name","in":"query"},{"name":"parentId","in":"query"},{"name":"originId","in":"query"},{"name":"templatesOnly","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["emails/builder.readonly"]}]
  }],
  ["create-template", {
    name: "create-template",
    description: `Create a new template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"title":{"type":"string"},"type":{"type":"string","enum":["html","folder","import","builder","blank"]},"updatedBy":{"type":"string"},"builderVersion":{"type":"string","enum":["1","2"],"default":"2"},"name":{"type":"string"},"parentId":{"type":"string"},"templateDataUrl":{"type":"string"},"importProvider":{"type":"string","enum":["mailchimp","active_campaign","kajabi"]},"importURL":{"type":"string"},"templateSource":{"type":"string"},"isPlainText":{"type":"boolean"}},"required":["locationId","type","importProvider"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/emails/builder",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["emails/builder.write"]}]
  }],
  ["delete-template", {
    name: "delete-template",
    description: `Delete a template`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string"},"templateId":{"type":"string"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["locationId","templateId","Version"]},
    method: "delete",
    pathTemplate: "/emails/builder/{locationId}/{templateId}",
    executionParameters: [{"name":"locationId","in":"path"},{"name":"templateId","in":"path"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["update-template", {
    name: "update-template",
    description: `Update a template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"templateId":{"type":"string"},"updatedBy":{"type":"string"},"dnd":{"allOf":[{"type":"object","properties":{"elements":{"type":"array","items":{"type":"string"}},"attrs":{"type":"object"},"templateSettings":{"type":"object","properties":{}}},"required":["elements","attrs","templateSettings"]}]},"html":{"type":"string"},"editorType":{"type":"string","enum":["html","builder"]},"previewText":{"type":"string"},"isPlainText":{"type":"boolean"}},"required":["locationId","templateId","updatedBy","dnd","html","editorType"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/emails/builder/data",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["emails/builder.write"]}]
  }],
  ["get-forms-submissions", {
    name: "get-forms-submissions",
    description: `Get Forms Submissions`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"page":{"default":1,"type":"number","description":"Page No. By default it will be 1"},"limit":{"default":20,"type":"number","description":"Limit Per Page records count. will allow maximum up to 100 and default will be 20"},"formId":{"type":"string","description":"Filter submission by form id"},"q":{"type":"string","description":"Filter by contactId, name, email or phone no."},"startAt":{"type":"string","description":"Get submission by starting of this date. By default it will be same date of last month(YYYY-MM-DD)."},"endAt":{"type":"string","description":"Get submission by ending of this date. By default it will be current date(YYYY-MM-DD)."}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/forms/submissions",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"page","in":"query"},{"name":"limit","in":"query"},{"name":"formId","in":"query"},{"name":"q","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["forms.readonly"]}]
  }],
  ["upload-to-custom-fields", {
    name: "upload-to-custom-fields",
    description: `Post the necessary fields for the API to upload files. The files need to be a buffer with the key "< custom_field_id >_< file_id >". <br /> Here custom field id is the ID of your custom field and file id is a randomly generated id (or uuid) <br /> There is support for multiple file uploads as well. Have multiple fields in the format mentioned.<br />File size is limited to 50 MB.<br /><br /> The allowed file types are: <br/> <ul><li>PDF</li><li>DOCX</li><li>DOC</li><li>JPG</li><li>JPEG</li><li>PNG</li><li>GIF</li><li>CSV</li><li>XLSX</li><li>XLS</li><li>MP4</li><li>MPEG</li><li>ZIP</li><li>RAR</li><li>TXT</li><li>SVG</li></ul> <br /><br /> The API will return the updated contact object.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"contactId":{"type":"string","description":"Contact ID to upload the file to."},"locationId":{"type":"string","description":"Location ID of the contact."},"requestBody":{"type":"string","description":"Request body (content type: multipart/form-data)"}},"required":["Version","contactId","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/forms/upload-custom-files",
    executionParameters: [{"name":"Version","in":"header"},{"name":"contactId","in":"query"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "multipart/form-data",
    securityRequirements: [{"bearer":["forms.write"]},{"Location-Access":["forms.write"]}]
  }],
  ["create-redirect", {
    name: "create-redirect",
    description: `The "Create Redirect" API Allows adding a new url redirect to the system. Use this endpoint to create a url redirect with the specified details. Ensure that the required information is provided in the request payload.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"domain":{"type":"string"},"path":{"type":"string"},"target":{"type":"string"},"action":{"type":"string","enum":["funnel","website","url","all"]}},"required":["locationId","domain","path","target","action"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/funnels/lookup/redirect",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["funnels/redirect.write"]}]
  }],
  ["delete-redirect-by-id", {
    name: "delete-redirect-by-id",
    description: `The "Delete Redirect By Id" API Allows deletion of a URL redirect from the system using its unique identifier. Use this endpoint to delete a URL redirect with the specified ID using details provided in the request payload.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"locationId":{"type":"string"}},"required":["Version","id","locationId"]},
    method: "delete",
    pathTemplate: "/funnels/lookup/redirect/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["funnels/redirect.write"]}]
  }],
  ["update-redirect-by-id", {
    name: "update-redirect-by-id",
    description: `The "Update Redirect By Id" API Allows updating an existing URL redirect in the system. Use this endpoint to modify a URL redirect with the specified ID using details provided in the request payload.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"requestBody":{"type":"object","properties":{"target":{"type":"string"},"action":{"type":"string","enum":["funnel","website","url","all"]},"locationId":{"type":"string"}},"required":["target","action","locationId"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "patch",
    pathTemplate: "/funnels/lookup/redirect/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["funnels/redirect.write"]}]
  }],
  ["fetch-redirects-list", {
    name: "fetch-redirects-list",
    description: `Retrieves a list of all URL redirects based on the given query parameters.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"limit":{"type":"number"},"offset":{"type":"number"},"search":{"type":"string"}},"required":["Version","locationId","limit","offset"]},
    method: "get",
    pathTemplate: "/funnels/lookup/redirect/list",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"search","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["funnels/redirect.readonly"]}]
  }],
  ["getFunnels", {
    name: "getFunnels",
    description: `Retrieves a list of all funnels based on the given query parameters.`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string"},"type":{"type":"string"},"category":{"type":"string"},"offset":{"type":"string"},"limit":{"type":"string"},"parentId":{"type":"string"},"name":{"type":"string"}},"required":["locationId"]},
    method: "get",
    pathTemplate: "/funnels/funnel/list",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"type","in":"query"},{"name":"category","in":"query"},{"name":"offset","in":"query"},{"name":"limit","in":"query"},{"name":"parentId","in":"query"},{"name":"name","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["getPagesByFunnelId", {
    name: "getPagesByFunnelId",
    description: `Retrieves a list of all funnel pages based on the given query parameters.`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string"},"funnelId":{"type":"string"},"name":{"type":"string"},"limit":{"type":"number"},"offset":{"type":"number"}},"required":["locationId","funnelId","limit","offset"]},
    method: "get",
    pathTemplate: "/funnels/page",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"funnelId","in":"query"},{"name":"name","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["getPagesCountByFunnelId", {
    name: "getPagesCountByFunnelId",
    description: `Retrieves count of all funnel pages based on the given query parameters.`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string"},"funnelId":{"type":"string"},"name":{"type":"string"}},"required":["locationId","funnelId"]},
    method: "get",
    pathTemplate: "/funnels/page/count",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"funnelId","in":"query"},{"name":"name","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["list-invoice-templates", {
    name: "list-invoice-templates",
    description: `API to get list of templates`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"status":{"type":"string","description":"status to be filtered"},"startAt":{"type":"string","description":"startAt in YYYY-MM-DD format"},"endAt":{"type":"string","description":"endAt in YYYY-MM-DD format"},"search":{"type":"string","description":"To search for an invoice by id / name / email / phoneNo"},"paymentMode":{"enum":["default","live","test"],"type":"string","description":"payment mode"},"limit":{"type":"string","description":"Limit the number of items to return"},"offset":{"type":"string","description":"Number of items to skip"}},"required":["Version","altId","altType","limit","offset"]},
    method: "get",
    pathTemplate: "/invoices/template",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"status","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"search","in":"query"},{"name":"paymentMode","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/template.readonly"]},{"Agency-Access":["invoices/template.readonly"]}]
  }],
  ["create-invoice-template", {
    name: "create-invoice-template",
    description: `API to create a template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"internal":{"type":"boolean"},"name":{"type":"string","description":"Name of the template"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string"},"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Invoice"},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string"},"title":{"type":"string","description":"Template title"},"tipsConfiguration":{"description":"Configuration for tips on invoices","allOf":[{"type":"object","properties":{"tipsPercentage":{"description":"Percentage of tips allowed","example":[5,10,15],"type":"array","items":{"type":"string"}},"tipsEnabled":{"type":"boolean","description":"Tips enabled status","example":true}},"required":["tipsPercentage","tipsEnabled"]}]},"lateFeesConfiguration":{"description":"Late fees configuration for the invoices","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]},"invoiceNumberPrefix":{"type":"string","description":"prefix for invoice number"},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"string"}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","businessDetails","currency","items"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/template",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/template.write"]},{"Agency-Access":["invoices/template.write"]}]
  }],
  ["get-invoice-template", {
    name: "get-invoice-template",
    description: `API to get an template by template id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","templateId","altId","altType"]},
    method: "get",
    pathTemplate: "/invoices/template/{templateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/template.readonly"]},{"Agency-Access":["invoices/template.readonly"]}]
  }],
  ["update-invoice-template", {
    name: "update-invoice-template",
    description: `API to update an template by template id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"internal":{"type":"boolean"},"name":{"type":"string","description":"Name of the template"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string"},"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string"},"title":{"type":"string","description":"Template title"},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","businessDetails","currency","items"],"description":"The JSON request body."}},"required":["Version","templateId","requestBody"]},
    method: "put",
    pathTemplate: "/invoices/template/{templateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/template.write"]},{"Agency-Access":["invoices/template.write"]}]
  }],
  ["delete-invoice-template", {
    name: "delete-invoice-template",
    description: `API to update an template by template id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","templateId","altId","altType"]},
    method: "delete",
    pathTemplate: "/invoices/template/{templateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/template.write"]},{"Agency-Access":["invoices/template.write"]}]
  }],
  ["update-invoice-template-late-fees-configuration", {
    name: "update-invoice-template-late-fees-configuration",
    description: `API to update template late fees configuration by template id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"lateFeesConfiguration":{"description":"late fees configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]}},"required":["altId","altType","lateFeesConfiguration"],"description":"The JSON request body."}},"required":["Version","templateId","requestBody"]},
    method: "patch",
    pathTemplate: "/invoices/template/{templateId}/late-fees-configuration",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["update-invoice-payment-methods-configuration", {
    name: "update-invoice-payment-methods-configuration",
    description: `API to update template late fees configuration by template id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","templateId","requestBody"]},
    method: "patch",
    pathTemplate: "/invoices/template/{templateId}/payment-methods-configuration",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["list-invoice-schedules", {
    name: "list-invoice-schedules",
    description: `API to get list of schedules`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"status":{"type":"string","description":"status to be filtered"},"startAt":{"type":"string","description":"startAt in YYYY-MM-DD format"},"endAt":{"type":"string","description":"endAt in YYYY-MM-DD format"},"search":{"type":"string","description":"To search for an invoice by id / name / email / phoneNo"},"paymentMode":{"enum":["default","live","test"],"type":"string","description":"payment mode"},"limit":{"type":"string","description":"Limit the number of items to return"},"offset":{"type":"string","description":"Number of items to skip"}},"required":["Version","altId","altType","limit","offset"]},
    method: "get",
    pathTemplate: "/invoices/schedule",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"status","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"search","in":"query"},{"name":"paymentMode","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/schedule.readonly"]},{"Agency-Access":["invoices/schedule.readonly"]}]
  }],
  ["create-invoice-schedule", {
    name: "create-invoice-schedule",
    description: `API to create an invoice Schedule`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string"},"contactDetails":{"type":"object","properties":{"id":{"type":"string","description":"Contact ID"},"name":{"type":"string","description":"Contact Name"},"phoneNo":{"type":"string","description":"Contact Phone Number"},"email":{"type":"string","description":"Contact Email"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name"},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1"},"addressLine2":{"type":"string","description":"Address Line 2"},"city":{"type":"string","description":"City"},"state":{"type":"string","description":"State"},"countryCode":{"type":"string","description":"Country Code"},"postalCode":{"type":"string","description":"Postal Code"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]},"schedule":{"type":"object","properties":{"executeAt":{"type":"string"},"rrule":{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly","minutely","secondly"]},"interval":{"type":"number"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DD format"},"startTime":{"type":"string","description":"Start time in HH:mm:ss format"},"endDate":{"type":"string","description":"End date in YYYY-MM-DD format"},"endTime":{"type":"string","description":"End time in HH:mm:ss format"},"dayOfMonth":{"type":"number","description":"-1, 1, 2, 3, ..., 27, 28"},"dayOfWeek":{"type":"string","enum":["mo","tu","we","th","fr","sa","su"]},"numOfWeek":{"type":"number","description":"-1, 1, 2, 3, 4"},"monthOfYear":{"type":"string","enum":["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]},"count":{"type":"number","description":"Max number of task executions"},"daysBefore":{"type":"number","description":"Execute task number of days before"}},"required":["intervalType","interval","startDate"]}}},"liveMode":{"type":"boolean"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string"},"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Invoice"},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string"},"title":{"type":"string"},"tipsConfiguration":{"description":"Configuration for tips on invoices","allOf":[{"type":"object","properties":{"tipsPercentage":{"description":"Percentage of tips allowed","example":[5,10,15],"type":"array","items":{"type":"string"}},"tipsEnabled":{"type":"boolean","description":"Tips enabled status","example":true}},"required":["tipsPercentage","tipsEnabled"]}]},"lateFeesConfiguration":{"description":"Late fees configuration for the invoices","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]},"invoiceNumberPrefix":{"type":"string","description":"prefix for invoice number"},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","contactDetails","schedule","liveMode","businessDetails","currency","items","discount"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/schedule",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["get-invoice-schedule", {
    name: "get-invoice-schedule",
    description: `API to get an schedule by schedule id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","scheduleId","altId","altType"]},
    method: "get",
    pathTemplate: "/invoices/schedule/{scheduleId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/schedule.readonly"]},{"Agency-Access":["invoices/schedule.readonly"]}]
  }],
  ["update-invoice-schedule", {
    name: "update-invoice-schedule",
    description: `API to update an schedule by schedule id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string"},"contactDetails":{"type":"object","properties":{"id":{"type":"string","description":"Contact ID"},"name":{"type":"string","description":"Contact Name"},"phoneNo":{"type":"string","description":"Contact Phone Number"},"email":{"type":"string","description":"Contact Email"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name"},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1"},"addressLine2":{"type":"string","description":"Address Line 2"},"city":{"type":"string","description":"City"},"state":{"type":"string","description":"State"},"countryCode":{"type":"string","description":"Country Code"},"postalCode":{"type":"string","description":"Postal Code"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]},"schedule":{"type":"object","properties":{"executeAt":{"type":"string"},"rrule":{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly","minutely","secondly"]},"interval":{"type":"number"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DD format"},"startTime":{"type":"string","description":"Start time in HH:mm:ss format"},"endDate":{"type":"string","description":"End date in YYYY-MM-DD format"},"endTime":{"type":"string","description":"End time in HH:mm:ss format"},"dayOfMonth":{"type":"number","description":"-1, 1, 2, 3, ..., 27, 28"},"dayOfWeek":{"type":"string","enum":["mo","tu","we","th","fr","sa","su"]},"numOfWeek":{"type":"number","description":"-1, 1, 2, 3, 4"},"monthOfYear":{"type":"string","enum":["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]},"count":{"type":"number","description":"Max number of task executions"},"daysBefore":{"type":"number","description":"Execute task number of days before"}},"required":["intervalType","interval","startDate"]}}},"liveMode":{"type":"boolean"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string"},"items":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string"},"title":{"type":"string"},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","contactDetails","schedule","liveMode","businessDetails","currency","items","discount"],"description":"The JSON request body."}},"required":["Version","scheduleId","requestBody"]},
    method: "put",
    pathTemplate: "/invoices/schedule/{scheduleId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["delete-invoice-schedule", {
    name: "delete-invoice-schedule",
    description: `API to delete an schedule by schedule id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","scheduleId","altId","altType"]},
    method: "delete",
    pathTemplate: "/invoices/schedule/{scheduleId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["update-and-schedule-invoice-schedule", {
    name: "update-and-schedule-invoice-schedule",
    description: `API to update scheduled recurring invoice`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"}},"required":["Version","scheduleId"]},
    method: "post",
    pathTemplate: "/invoices/schedule/{scheduleId}/updateAndSchedule",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["schedule-invoice-schedule", {
    name: "schedule-invoice-schedule",
    description: `API to schedule an schedule invoice to start sending to the customer`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"liveMode":{"type":"boolean"},"autoPayment":{"description":"auto-payment configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean"},"type":{"type":"string"},"paymentMethodId":{"type":"string"},"customerId":{"type":"string"},"card":{"type":"object","properties":{"type":{"type":"string","enum":["visa","mastercard","other"],"example":"mastercard"},"last4":{"type":"string","description":"Last 4 digit of the card","example":"1234"}},"required":["type","last4"]},"usBankAccount":{"type":"object","properties":{"bank_name":{"type":"string"},"last4":{"type":"string"}},"required":["bank_name","last4"]},"sepaDirectDebit":{"type":"object","properties":{"bank_code":{"type":"string"},"last4":{"type":"string"},"branch_code":{"type":"string"}},"required":["bank_code","last4","branch_code"]},"bacsDirectDebit":{"type":"object","properties":{"sort_code":{"type":"string"},"last4":{"type":"string"}},"required":["sort_code","last4"]},"becsDirectDebit":{"type":"object","properties":{"bsb_number":{"type":"string"},"last4":{"type":"string"}},"required":["bsb_number","last4"]},"cardId":{"type":"string"}},"required":["enable"]}]}},"required":["altId","altType","liveMode"],"description":"The JSON request body."}},"required":["Version","scheduleId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/schedule/{scheduleId}/schedule",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["auto-payment-invoice-schedule", {
    name: "auto-payment-invoice-schedule",
    description: `API to manage auto payment for a schedule`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"id":{"type":"string"},"autoPayment":{"description":"auto-payment configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean"},"type":{"type":"string"},"paymentMethodId":{"type":"string"},"customerId":{"type":"string"},"card":{"type":"object","properties":{"type":{"type":"string","enum":["visa","mastercard","other"],"example":"mastercard"},"last4":{"type":"string","description":"Last 4 digit of the card","example":"1234"}},"required":["type","last4"]},"usBankAccount":{"type":"object","properties":{"bank_name":{"type":"string"},"last4":{"type":"string"}},"required":["bank_name","last4"]},"sepaDirectDebit":{"type":"object","properties":{"bank_code":{"type":"string"},"last4":{"type":"string"},"branch_code":{"type":"string"}},"required":["bank_code","last4","branch_code"]},"bacsDirectDebit":{"type":"object","properties":{"sort_code":{"type":"string"},"last4":{"type":"string"}},"required":["sort_code","last4"]},"becsDirectDebit":{"type":"object","properties":{"bsb_number":{"type":"string"},"last4":{"type":"string"}},"required":["bsb_number","last4"]},"cardId":{"type":"string"}},"required":["enable"]}]}},"required":["altId","altType","id","autoPayment"],"description":"The JSON request body."}},"required":["Version","scheduleId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/schedule/{scheduleId}/auto-payment",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["cancel-invoice-schedule", {
    name: "cancel-invoice-schedule",
    description: `API to cancel a scheduled invoice by schedule id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"scheduleId":{"type":"string","description":"Schedule Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","scheduleId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/schedule/{scheduleId}/cancel",
    executionParameters: [{"name":"Version","in":"header"},{"name":"scheduleId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/schedule.write"]},{"Agency-Access":["invoices/schedule.write"]}]
  }],
  ["text2pay-invoice", {
    name: "text2pay-invoice",
    description: `API to create or update a text2pay invoice`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string","description":"Invoice Name"},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the invoice.","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the invoice"},"contactDetails":{"description":"Contact information to send the invoice to","allOf":[{"type":"object","properties":{"id":{"type":"string","description":"Contact ID","example":"6578278e879ad2646715ba9c"},"name":{"type":"string","description":"Contact Name","example":"Alex"},"phoneNo":{"type":"string","description":"Contact Phone Number","example":"+1234567890"},"email":{"type":"string","description":"Contact Email","example":"alex@example.com"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string","example":"alex@example.com"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name","example":"ABC Corp."},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]}]},"invoiceNumber":{"type":"string","description":"Invoice Number"},"issueDate":{"type":"string","description":"Issue date in YYYY-MM-DD format"},"dueDate":{"type":"string","description":"Due date in YYYY-MM-DD format"},"sentTo":{"type":"object","properties":{"email":{"description":"Email Address","type":"array","items":{"type":"string"}},"emailCc":{"description":"cc to be kept in any sent out emails","type":"array","items":{"type":"string"}},"emailBcc":{"description":"bcc to be kept in any sent out emails","type":"array","items":{"type":"string"}},"phoneNo":{"description":"Contact Phone Number","type":"array","items":{"type":"string"}}},"required":["email"]},"liveMode":{"type":"boolean"},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Invoice"},"paymentSchedule":{"description":"split invoice into payment schedule summing up to full invoice amount","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Payment schedule type","enum":["fixed","percentage"],"example":"percentage"},"schedules":{"description":"payment schedule item","type":"array","items":{"type":"string"}}},"required":["type","schedules"]}]},"lateFeesConfiguration":{"description":"late fees configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]},"tipsConfiguration":{"description":"tips configuration for the invoice","allOf":[{"type":"object","properties":{"tipsPercentage":{"description":"Percentage of tips allowed","example":[5,10,15],"type":"array","items":{"type":"string"}},"tipsEnabled":{"type":"boolean","description":"Tips enabled status","example":true}},"required":["tipsPercentage","tipsEnabled"]}]},"invoiceNumberPrefix":{"type":"string","description":"prefix for invoice number"},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]},"id":{"type":"string","description":"id of invoice to update. If skipped, a new invoice will be created"},"includeTermsNote":{"type":"boolean","description":"include terms & notes with receipts"},"action":{"type":"string","description":"create invoice in draft mode or send mode","enum":["draft","send"]},"userId":{"type":"string","description":"id of user generating invoice"},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}}},"required":["altId","altType","name","currency","items","contactDetails","issueDate","sentTo","liveMode","action","userId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/text2pay",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]}]
  }],
  ["generate-invoice-number", {
    name: "generate-invoice-number",
    description: `Get the next invoice number for the given location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/invoices/generate-invoice-number",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices.readonly"]},{"Agency-Access":["invoices.readonly"]}]
  }],
  ["get-invoice", {
    name: "get-invoice",
    description: `API to get invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","invoiceId","altId","altType"]},
    method: "get",
    pathTemplate: "/invoices/{invoiceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices.readonly"]},{"Agency-Access":["invoices.readonly"]}]
  }],
  ["update-invoice", {
    name: "update-invoice",
    description: `API to update invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string","description":"Name to be updated"},"title":{"type":"string","description":"Title for the invoice"},"currency":{"type":"string","description":"Currency"},"description":{"type":"string","description":"Description"},"businessDetails":{"description":"Business details which need to be updated","allOf":[{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL","example":"https://example.com/logo.png"},"name":{"type":"string","description":"Business Name","example":"ABC Corp."},"phoneNo":{"type":"string","description":"Business Phone Number","example":"+1-214-559-6993"},"address":{"description":"Business Address","example":"9931 Beechwood, TX","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link","example":"wwww.example.com"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}}]},"invoiceNumber":{"type":"string","description":"Invoice Number"},"contactId":{"type":"string","description":"Id of the contact which you need to send the invoice"},"contactDetails":{"type":"object","properties":{"id":{"type":"string","description":"Contact ID"},"name":{"type":"string","description":"Contact Name"},"phoneNo":{"type":"string","description":"Contact Phone Number"},"email":{"type":"string","description":"Contact Email"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name"},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1"},"addressLine2":{"type":"string","description":"Address Line 2"},"city":{"type":"string","description":"City"},"state":{"type":"string","description":"State"},"countryCode":{"type":"string","description":"Country Code"},"postalCode":{"type":"string","description":"Postal Code"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"invoiceItems":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Invoice"},"liveMode":{"type":"boolean","description":"Payment mode"},"issueDate":{"type":"string","description":"Issue date in YYYY-MM-DD format"},"dueDate":{"type":"string","description":"Due date in YYYY-MM-DD format"},"paymentSchedule":{"description":"split invoice into payment schedule summing up to full invoice amount","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Payment schedule type","enum":["fixed","percentage"],"example":"percentage"},"schedules":{"description":"payment schedule item","type":"array","items":{"type":"string"}}},"required":["type","schedules"]}]},"tipsConfiguration":{"description":"tips configuration for the invoice","allOf":[{"type":"object","properties":{"tipsPercentage":{"description":"Percentage of tips allowed","example":[5,10,15],"type":"array","items":{"type":"string"}},"tipsEnabled":{"type":"boolean","description":"Tips enabled status","example":true}},"required":["tipsPercentage","tipsEnabled"]}]},"xeroDetails":{"type":"object"},"invoiceNumberPrefix":{"type":"string","description":"prefix for invoice number"},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","currency","invoiceItems","issueDate","dueDate"],"description":"The JSON request body."}},"required":["Version","invoiceId","requestBody"]},
    method: "put",
    pathTemplate: "/invoices/{invoiceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["delete-invoice", {
    name: "delete-invoice",
    description: `API to delete invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"}},"required":["Version","invoiceId","altId","altType"]},
    method: "delete",
    pathTemplate: "/invoices/{invoiceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["update-invoice-late-fees-configuration", {
    name: "update-invoice-late-fees-configuration",
    description: `API to update invoice late fees configuration by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"lateFeesConfiguration":{"description":"late fees configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]}},"required":["altId","altType","lateFeesConfiguration"],"description":"The JSON request body."}},"required":["Version","invoiceId","requestBody"]},
    method: "patch",
    pathTemplate: "/invoices/{invoiceId}/late-fees-configuration",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["void-invoice", {
    name: "void-invoice",
    description: `API to delete invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","invoiceId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/{invoiceId}/void",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["send-invoice", {
    name: "send-invoice",
    description: `API to send invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates."},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean"},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"autoPayment":{"description":"auto-payment configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean"},"type":{"type":"string"},"paymentMethodId":{"type":"string"},"customerId":{"type":"string"},"card":{"type":"object","properties":{"type":{"type":"string","enum":["visa","mastercard","other"],"example":"mastercard"},"last4":{"type":"string","description":"Last 4 digit of the card","example":"1234"}},"required":["type","last4"]},"usBankAccount":{"type":"object","properties":{"bank_name":{"type":"string"},"last4":{"type":"string"}},"required":["bank_name","last4"]},"sepaDirectDebit":{"type":"object","properties":{"bank_code":{"type":"string"},"last4":{"type":"string"},"branch_code":{"type":"string"}},"required":["bank_code","last4","branch_code"]},"bacsDirectDebit":{"type":"object","properties":{"sort_code":{"type":"string"},"last4":{"type":"string"}},"required":["sort_code","last4"]},"becsDirectDebit":{"type":"object","properties":{"bsb_number":{"type":"string"},"last4":{"type":"string"}},"required":["bsb_number","last4"]},"cardId":{"type":"string"}},"required":["enable"]}]}},"required":["altId","altType","userId","action","liveMode"],"description":"The JSON request body."}},"required":["Version","invoiceId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/{invoiceId}/send",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["record-invoice", {
    name: "record-invoice",
    description: `API to record manual payment for an invoice by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"invoiceId":{"type":"string","description":"Invoice Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"mode":{"type":"string","description":"manual payment method","enum":["cash","card","cheque","bank_transfer","other"]},"card":{"type":"object","properties":{"type":{"type":"string","enum":["visa","mastercard","other"]},"last4":{"type":"string","description":"Last 4 digit of the card"}},"required":["type","last4"]},"cheque":{"type":"object","properties":{"number":{"type":"string","description":"check number"}},"required":["number"]},"notes":{"type":"string","description":"Any note to be recorded with the transaction"},"amount":{"type":"number","description":"Amount to be paid against the invoice."},"meta":{"type":"object"},"paymentScheduleIds":{"description":"Payment Schedule Ids to be recorded against the invoice.","type":"array","items":{"type":"string"}},"fulfilledAt":{"type":"string","description":"Updated At to be recorded against the invoice."}},"required":["altId","altType","mode","card","cheque","notes"],"description":"The JSON request body."}},"required":["Version","invoiceId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/{invoiceId}/record-payment",
    executionParameters: [{"name":"Version","in":"header"},{"name":"invoiceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["update-invoice-last-visited-at", {
    name: "update-invoice-last-visited-at",
    description: `API to update invoice last visited at by invoice id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"invoiceId":{"type":"string","description":"Invoice Id"}},"required":["invoiceId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "patch",
    pathTemplate: "/invoices/stats/last-visited-at",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["create-new-estimate", {
    name: "create-new-estimate",
    description: `Create a new estimate with the provided details`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Estimate Name"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the estimate.","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"liveMode":{"type":"boolean","description":"livemode for estimate","default":true},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the estimate"},"contactDetails":{"description":"Contact information to send the estimate to","allOf":[{"type":"object","properties":{"id":{"type":"string","description":"Contact ID","example":"6578278e879ad2646715ba9c"},"name":{"type":"string","description":"Contact Name","example":"Alex"},"phoneNo":{"type":"string","description":"Contact Phone Number","example":"+1234567890"},"email":{"type":"string","description":"Contact Email","example":"alex@example.com"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string","example":"alex@example.com"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name","example":"ABC Corp."},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]}]},"estimateNumber":{"type":"number","description":"Estimate Number, if not specified will take in the next valid estimate number"},"issueDate":{"type":"string","description":"issue date estimate"},"expiryDate":{"type":"string","description":"expiry date estimate"},"sentTo":{"description":"Email and sent to details for the estimate","allOf":[{"type":"object","properties":{"email":{"description":"Email Address","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"emailCc":{"description":"cc to be kept in any sent out emails","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"emailBcc":{"description":"bcc to be kept in any sent out emails","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"phoneNo":{"description":"Contact Phone Number","example":["+1-214-559-6993"],"type":"array","items":{"type":"string"}}},"required":["email"]}]},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Estimate","default":false},"meta":{"type":"object","description":"Meta data for the estimate"},"sendEstimateDetails":{"description":"When sending estimate directly while saving","allOf":[{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id","example":"6578278e879ad2646715ba9c"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean","description":"livemode for estimate","example":true},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates.","example":"6578278e879ad2646715ba9c"},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"estimateName":{"type":"string","description":"estimate name","example":"Estimate"}},"required":["altId","altType","action","liveMode","userId"]}]},"frequencySettings":{"description":"frequency settings for the estimate","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"enabled for the frequency settings","example":true},"schedule":{"description":"schedule setting for the estimate","allOf":[{"type":"object","properties":{"executeAt":{"type":"string"},"rrule":{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly","minutely","secondly"],"example":"monthly"},"interval":{"type":"number","example":2},"startDate":{"type":"string","description":"Start date in YYYY-MM-DD format","example":"2023-01-01"},"startTime":{"type":"string","description":"Start time in HH:mm:ss format","example":"20:45:00"},"endDate":{"type":"string","description":"End date in YYYY-MM-DD format","example":"2029-11-01"},"endTime":{"type":"string","description":"End time in HH:mm:ss format","example":"18:45:00"},"dayOfMonth":{"type":"number","description":"-1, 1, 2, 3, ..., 27, 28","example":15},"dayOfWeek":{"type":"string","enum":["mo","tu","we","th","fr","sa","su"],"example":"mo"},"numOfWeek":{"type":"number","description":"-1, 1, 2, 3, 4","example":-1},"monthOfYear":{"type":"string","enum":["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"],"example":"jan"},"count":{"type":"number","description":"Max number of task executions","example":10},"daysBefore":{"type":"number","description":"Execute task number of days before","example":5}},"required":["intervalType","interval","startDate"]}}}]}},"required":["enabled","schedule"]}]},"estimateNumberPrefix":{"type":"string","description":"Prefix for the estimate number","default":"EST-"},"userId":{"type":"string","description":"User Id"},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"autoInvoice":{"description":"Auto invoice for the estimate","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Enable Auto Invoice","example":true},"directPayments":{"type":"boolean","description":"Direct Payments","example":true}},"required":["enabled"]}]},"miscellaneousCharges":{"description":"miscellaneous charges for the estimate","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]},"paymentScheduleConfig":{"description":"Payment Schedule Config for the estimate","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Payment Schedule Type","example":"fixed","enum":["fixed","percentage"]},"dateConfig":{"description":"Due date type configuration","allOf":[{"type":"object","properties":{"depositDateType":{"type":"string","description":"Deposit date type","example":"estimate_accepted","enum":["estimate_accepted","custom"]},"scheduleDateType":{"type":"string","description":"Payment Schedule Date Type","example":"regular_interval","enum":["regular_interval","custom"]}},"required":["depositDateType","scheduleDateType"]}]},"schedules":{"description":"Payment Schedule Items","items":{"type":"array","items":{"type":"object"}},"type":"array"}},"required":["type","dateConfig","schedules"]}]}},"required":["altId","altType","name","businessDetails","currency","items","discount","contactDetails","frequencySettings"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/estimate",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["update-estimate", {
    name: "update-estimate",
    description: `Update an existing estimate with new details`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"estimateId":{"type":"string","description":"Estimate Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Estimate Name"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the estimate.","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"liveMode":{"type":"boolean","description":"livemode for estimate","default":true},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the estimate"},"contactDetails":{"description":"Contact information to send the estimate to","allOf":[{"type":"object","properties":{"id":{"type":"string","description":"Contact ID","example":"6578278e879ad2646715ba9c"},"name":{"type":"string","description":"Contact Name","example":"Alex"},"phoneNo":{"type":"string","description":"Contact Phone Number","example":"+1234567890"},"email":{"type":"string","description":"Contact Email","example":"alex@example.com"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string","example":"alex@example.com"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name","example":"ABC Corp."},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]}]},"estimateNumber":{"type":"number","description":"Estimate Number, if not specified will take in the next valid estimate number"},"issueDate":{"type":"string","description":"issue date estimate"},"expiryDate":{"type":"string","description":"expiry date estimate"},"sentTo":{"description":"Email and sent to details for the estimate","allOf":[{"type":"object","properties":{"email":{"description":"Email Address","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"emailCc":{"description":"cc to be kept in any sent out emails","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"emailBcc":{"description":"bcc to be kept in any sent out emails","example":["alex@example.com"],"type":"array","items":{"type":"string"}},"phoneNo":{"description":"Contact Phone Number","example":["+1-214-559-6993"],"type":"array","items":{"type":"string"}}},"required":["email"]}]},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Estimate","default":false},"meta":{"type":"object","description":"Meta data for the estimate"},"sendEstimateDetails":{"description":"When sending estimate directly while saving","allOf":[{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id","example":"6578278e879ad2646715ba9c"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean","description":"livemode for estimate","example":true},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates.","example":"6578278e879ad2646715ba9c"},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"estimateName":{"type":"string","description":"estimate name","example":"Estimate"}},"required":["altId","altType","action","liveMode","userId"]}]},"frequencySettings":{"description":"frequency settings for the estimate","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"enabled for the frequency settings","example":true},"schedule":{"description":"schedule setting for the estimate","allOf":[{"type":"object","properties":{"executeAt":{"type":"string"},"rrule":{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly","minutely","secondly"],"example":"monthly"},"interval":{"type":"number","example":2},"startDate":{"type":"string","description":"Start date in YYYY-MM-DD format","example":"2023-01-01"},"startTime":{"type":"string","description":"Start time in HH:mm:ss format","example":"20:45:00"},"endDate":{"type":"string","description":"End date in YYYY-MM-DD format","example":"2029-11-01"},"endTime":{"type":"string","description":"End time in HH:mm:ss format","example":"18:45:00"},"dayOfMonth":{"type":"number","description":"-1, 1, 2, 3, ..., 27, 28","example":15},"dayOfWeek":{"type":"string","enum":["mo","tu","we","th","fr","sa","su"],"example":"mo"},"numOfWeek":{"type":"number","description":"-1, 1, 2, 3, 4","example":-1},"monthOfYear":{"type":"string","enum":["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"],"example":"jan"},"count":{"type":"number","description":"Max number of task executions","example":10},"daysBefore":{"type":"number","description":"Execute task number of days before","example":5}},"required":["intervalType","interval","startDate"]}}}]}},"required":["enabled","schedule"]}]},"estimateNumberPrefix":{"type":"string","description":"Prefix for the estimate number","default":"EST-"},"userId":{"type":"string","description":"User Id"},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"autoInvoice":{"description":"Auto invoice for the estimate","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Enable Auto Invoice","example":true},"directPayments":{"type":"boolean","description":"Direct Payments","example":true}},"required":["enabled"]}]},"miscellaneousCharges":{"description":"miscellaneous charges for the estimate","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]},"paymentScheduleConfig":{"description":"Payment Schedule Config for the estimate","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Payment Schedule Type","example":"fixed","enum":["fixed","percentage"]},"dateConfig":{"description":"Due date type configuration","allOf":[{"type":"object","properties":{"depositDateType":{"type":"string","description":"Deposit date type","example":"estimate_accepted","enum":["estimate_accepted","custom"]},"scheduleDateType":{"type":"string","description":"Payment Schedule Date Type","example":"regular_interval","enum":["regular_interval","custom"]}},"required":["depositDateType","scheduleDateType"]}]},"schedules":{"description":"Payment Schedule Items","items":{"type":"array","items":{"type":"object"}},"type":"array"}},"required":["type","dateConfig","schedules"]}]},"estimateStatus":{"type":"string","description":"Estimate Status","enum":["all","draft","sent","accepted","declined","invoiced","viewed"]}},"required":["altId","altType","name","businessDetails","currency","items","discount","contactDetails","frequencySettings"],"description":"The JSON request body."}},"required":["Version","estimateId","requestBody"]},
    method: "put",
    pathTemplate: "/invoices/estimate/{estimateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"estimateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["delete-estimate", {
    name: "delete-estimate",
    description: `Delete an existing estimate`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"estimateId":{"type":"string","description":"Estimate Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","estimateId","requestBody"]},
    method: "delete",
    pathTemplate: "/invoices/estimate/{estimateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"estimateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["generate-estimate-number", {
    name: "generate-estimate-number",
    description: `Get the next estimate number for the given location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/invoices/estimate/number/generate",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/estimate.readonly"]},{"Agency-Access":["invoices/estimate.readonly"]}]
  }],
  ["send-estimate", {
    name: "send-estimate",
    description: `API to send estimate by estimate id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"estimateId":{"type":"string","description":"Estimate Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean","description":"livemode for estimate"},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates."},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"estimateName":{"type":"string","description":"estimate name"}},"required":["altId","altType","action","liveMode","userId"],"description":"The JSON request body."}},"required":["Version","estimateId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/estimate/{estimateId}/send",
    executionParameters: [{"name":"Version","in":"header"},{"name":"estimateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["create-invoice-from-estimate", {
    name: "create-invoice-from-estimate",
    description: `Create a new invoice from an existing estimate`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"estimateId":{"type":"string","description":"Estimate Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"markAsInvoiced":{"type":"boolean","description":"Mark Estimate as Invoiced"},"version":{"type":"string","description":"Version of the update request","enum":["v1","v2"]}},"required":["altId","altType","markAsInvoiced"],"description":"The JSON request body."}},"required":["Version","estimateId","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/estimate/{estimateId}/invoice",
    executionParameters: [{"name":"Version","in":"header"},{"name":"estimateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["list-estimates", {
    name: "list-estimates",
    description: `Get a paginated list of estimates`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"startAt":{"type":"string","description":"startAt in YYYY-MM-DD format"},"endAt":{"type":"string","description":"endAt in YYYY-MM-DD format"},"search":{"type":"string","description":"search text for estimates name"},"status":{"enum":["all","draft","sent","accepted","declined","invoiced","viewed"],"type":"string","description":"estimate status"},"contactId":{"type":"string","description":"Contact ID for the estimate"},"limit":{"type":"string","description":"Limit the number of items to return"},"offset":{"type":"string","description":"Number of items to skip"}},"required":["Version","altId","altType","limit","offset"]},
    method: "get",
    pathTemplate: "/invoices/estimate/list",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"search","in":"query"},{"name":"status","in":"query"},{"name":"contactId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/estimate.readonly"]},{"Agency-Access":["invoices/estimate.readonly"]}]
  }],
  ["update-estimate-last-visited-at", {
    name: "update-estimate-last-visited-at",
    description: `API to update estimate last visited at by estimate id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"estimateId":{"type":"string","description":"Estimate Id"}},"required":["estimateId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "patch",
    pathTemplate: "/invoices/estimate/stats/last-visited-at",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["list-estimate-templates", {
    name: "list-estimate-templates",
    description: `Get a list of estimate templates or a specific template by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"search":{"type":"string","description":"To search for an estimate template by id / name"},"limit":{"type":"string","description":"Limit the number of items to return"},"offset":{"type":"string","description":"Number of items to skip"}},"required":["Version","altId","altType","limit","offset"]},
    method: "get",
    pathTemplate: "/invoices/estimate/template",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"search","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/estimate.readonly"]},{"Agency-Access":["invoices/estimate.readonly"]}]
  }],
  ["create-estimate-template", {
    name: "create-estimate-template",
    description: `Create a new estimate template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Estimate Name"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the estimate.","items":{"type":"array","items":{"type":"object"}},"type":"array"},"liveMode":{"type":"boolean","description":"livemode for estimate","default":true},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the estimate"},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Estimate","default":false},"meta":{"type":"object","description":"Meta data for the estimate"},"sendEstimateDetails":{"description":"When sending estimate directly while saving","allOf":[{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id","example":"6578278e879ad2646715ba9c"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean","description":"livemode for estimate","example":true},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates.","example":"6578278e879ad2646715ba9c"},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"estimateName":{"type":"string","description":"estimate name","example":"Estimate"}},"required":["altId","altType","action","liveMode","userId"]}]},"estimateNumberPrefix":{"type":"string","description":"Prefix for the estimate number","default":"EST-"},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the estimate","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","businessDetails","currency","items","discount"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices/estimate/template",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["update-estimate-template", {
    name: "update-estimate-template",
    description: `Update an existing estimate template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Estimate Name"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the estimate.","items":{"type":"array","items":{"type":"object"}},"type":"array"},"liveMode":{"type":"boolean","description":"livemode for estimate","default":true},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the estimate"},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Estimate","default":false},"meta":{"type":"object","description":"Meta data for the estimate"},"sendEstimateDetails":{"description":"When sending estimate directly while saving","allOf":[{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id","example":"6578278e879ad2646715ba9c"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","enum":["sms_and_email","send_manually","email","sms"]},"liveMode":{"type":"boolean","description":"livemode for estimate","example":true},"userId":{"type":"string","description":"Please ensure that the UserId corresponds to an authorized personnel, either by an employee ID or agency ID, to access this location. This account will serve as the primary channel for all future communications and updates.","example":"6578278e879ad2646715ba9c"},"sentFrom":{"description":"sender details for invoice, valid only if invoice is not sent manually","allOf":[{"type":"object","properties":{"fromName":{"type":"string","description":"Sender name to be used while sending invoice","example":"Alex"},"fromEmail":{"type":"string","description":"Email id to be used while sending out invoices","example":"alex@example.com"}}}]},"estimateName":{"type":"string","description":"estimate name","example":"Estimate"}},"required":["altId","altType","action","liveMode","userId"]}]},"estimateNumberPrefix":{"type":"string","description":"Prefix for the estimate number","default":"EST-"},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the estimate","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","businessDetails","currency","items","discount"],"description":"The JSON request body."}},"required":["Version","templateId","requestBody"]},
    method: "put",
    pathTemplate: "/invoices/estimate/template/{templateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["delete-estimate-template", {
    name: "delete-estimate-template",
    description: `Delete an existing estimate template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"templateId":{"type":"string","description":"Template Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","templateId","requestBody"]},
    method: "delete",
    pathTemplate: "/invoices/estimate/template/{templateId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"templateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices/estimate.write"]},{"Agency-Access":["invoices/estimate.write"]}]
  }],
  ["preview-estimate-template", {
    name: "preview-estimate-template",
    description: `Get a preview of an estimate template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"templateId":{"type":"string","description":"Template Id"}},"required":["Version","altId","altType","templateId"]},
    method: "get",
    pathTemplate: "/invoices/estimate/template/preview",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"templateId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices/estimate.readonly"]},{"Agency-Access":["invoices/estimate.readonly"]}]
  }],
  ["get-link-by-id", {
    name: "get-link-by-id",
    description: `Get a single link by its ID`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"linkId":{"type":"string","description":"Link Id"}},"required":["Authorization","Version","locationId","linkId"]},
    method: "get",
    pathTemplate: "/links/id/{linkId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"linkId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-link", {
    name: "update-link",
    description: `Update Link`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"linkId":{"type":"string","description":"Link Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"redirectTo":{"type":"string"}},"required":["name","redirectTo"],"description":"The JSON request body."}},"required":["Version","linkId","requestBody"]},
    method: "put",
    pathTemplate: "/links/{linkId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"linkId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["links.write"]}]
  }],
  ["delete-link", {
    name: "delete-link",
    description: `Delete Link`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"linkId":{"type":"string","description":"Link Id"}},"required":["Version","linkId"]},
    method: "delete",
    pathTemplate: "/links/{linkId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"linkId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["links.write"]}]
  }],
  ["search-trigger-links", {
    name: "search-trigger-links",
    description: `Get list of links by searching`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"query":{"type":"string","description":"Search query as a string"},"skip":{"default":0,"type":"number","description":"Numbers of query results to skip"},"limit":{"default":20,"type":"number","description":"Limit on number of search results"}},"required":["Authorization","Version","locationId"]},
    method: "get",
    pathTemplate: "/links/search",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"query","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["search-locations", {
    name: "search-locations",
    description: `Search Sub-Account (Formerly Location)`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"companyId":{"type":"string","description":"The company/agency id on which you want to perform the search"},"skip":{"default":"0","type":"string","description":"The value by which the results should be skipped. Default will be 0"},"limit":{"default":"10","type":"string","description":"The value by which the results should be limited. Default will be 10"},"order":{"default":"asc","type":"string","description":"The order in which the results should be returned - Allowed values asc, desc. Default will be asc"},"email":{"type":"string"}},"required":["Version"]},
    method: "get",
    pathTemplate: "/locations/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"order","in":"query"},{"name":"email","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["locations.readonly"]},{"Location-Access":["locations.readonly"]}]
  }],
  ["get-location", {
    name: "get-location",
    description: `Get details of a Sub-Account (Formerly Location) by passing the sub-account id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["locations.readonly"]},{"Agency-Access":["locations.readonly"]}]
  }],
  ["put-location", {
    name: "put-location",
    description: `Update a Sub-Account (Formerly Location) based on the data provided`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name for the sub-account/location"},"phone":{"type":"string","description":"The phone number of the business for which sub-account is created"},"companyId":{"type":"string","description":"Company/Agency Id"},"address":{"type":"string","description":"The address of the business for which sub-account is created"},"city":{"type":"string","description":"The city where the business is located for which sub-account is created"},"state":{"type":"string","description":"The state in which the business operates for which sub-account is created"},"country":{"type":"string","description":"The country in which the business is present for which sub-account is created","enum":["AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","GB","UA","AE","US","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"postalCode":{"type":"string","description":"The postal code of the business for which sub-account is created"},"website":{"type":"string","description":"The website of the business for which sub-account is created"},"timezone":{"type":"string","description":"The timezone of the business for which sub-account is created"},"prospectInfo":{"allOf":[{"type":"object","properties":{"firstName":{"type":"string","description":"First name of the prospect","example":"John"},"lastName":{"type":"string","description":"Last name of the prospect","example":"Doe"},"email":{"type":"string","description":"Email of the prospect","example":"john.doe@mail.com"}},"required":["firstName","lastName","email"]}]},"settings":{"description":"The default settings for location","allOf":[{"type":"object","properties":{"allowDuplicateContact":{"type":"boolean","example":false},"allowDuplicateOpportunity":{"type":"boolean","example":false},"allowFacebookNameMerge":{"type":"boolean","example":false},"disableContactTimezone":{"type":"boolean","example":false}}}]},"social":{"description":"The social media links for location","allOf":[{"type":"object","properties":{"facebookUrl":{"type":"string","description":"Facebook URL","example":"https://www.facebook.com/"},"googlePlus":{"type":"string","description":"Googleplus URL","example":"https://www.googleplus.com/"},"linkedIn":{"type":"string","description":"LinkedIn URL","example":"https://www.linkedIn.com/"},"foursquare":{"type":"string","description":"Foursquare URL","example":"https://www.foursquare.com/"},"twitter":{"type":"string","description":"Twitter URL","example":"https://www.foutwitterrsquare.com/"},"yelp":{"type":"string","description":"Yelp URL","example":"https://www.yelp.com/"},"instagram":{"type":"string","description":"Instagram URL","example":"https://www.instagram.com/"},"youtube":{"type":"string","description":"Instagram URL","example":"https://www.youtube.com/"},"pinterest":{"type":"string","description":"Instagram URL","example":"https://www.pinterest.com/"},"blogRss":{"type":"string","description":"Instagram URL","example":"https://www.blogRss.com/"},"googlePlacesId":{"type":"string","description":"Google Business Places ID","example":"ChIJJGPdVbQTrjsRGUkefteUeFk"}}}]},"twilio":{"description":"The twilio credentials for location","allOf":[{"type":"object","properties":{"sid":{"type":"string","description":"SID provided by Twilio","example":"AC_XXXXXXXXXXX"},"authToken":{"type":"string","description":"Auth token provided by Twilio","example":"77_XXXXXXXXXXX"}},"required":["sid","authToken"]}]},"mailgun":{"description":"The mailgun credentials for location","allOf":[{"type":"object","properties":{"apiKey":{"type":"string","description":"API key provided by Mailgun","example":"key-XXXXXXXXXXX"},"domain":{"type":"string","description":"Domain connected with Mailgun","example":"replies.yourdomain.com"}},"required":["apiKey","domain"]}]},"snapshot":{"description":"The snapshot to be updated in the location.","allOf":[{"type":"object","properties":{"id":{"type":"string","description":"Snaptshot ID","example":"XXXXXXXXXXX"},"override":{"type":"boolean","description":"If you want override all conflicted assets then pass true. Default value is false.","example":false,"default":false}},"required":["id"]}]}},"required":["companyId"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "put",
    pathTemplate: "/locations/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["locations.write"]}]
  }],
  ["delete-location", {
    name: "delete-location",
    description: `Delete a Sub-Account (Formerly Location) from the Agency`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"deleteTwilioAccount":{"type":"boolean","description":"Boolean value to indicate whether to delete Twilio Account or not"}},"required":["Version","locationId","deleteTwilioAccount"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"deleteTwilioAccount","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["locations.internal-access-only"]}]
  }],
  ["get-location-tags", {
    name: "get-location-tags",
    description: `Get Sub-Account (Formerly Location) Tags`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/tags",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/tags.readonly"]}]
  }],
  ["create-tag", {
    name: "create-tag",
    description: `Create tag`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"Tag name"}},"required":["name"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/tags",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/tags.write"]}]
  }],
  ["get-tag-by-id", {
    name: "get-tag-by-id",
    description: `Get tag by id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"tagId":{"type":"string","description":"Tag Id"}},"required":["Version","locationId","tagId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/tags/{tagId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"tagId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-tag", {
    name: "update-tag",
    description: `Update tag`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"tagId":{"type":"string","description":"Tag Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"Tag name"}},"required":["name"],"description":"The JSON request body."}},"required":["Version","locationId","tagId","requestBody"]},
    method: "put",
    pathTemplate: "/locations/{locationId}/tags/{tagId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"tagId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-tag", {
    name: "delete-tag",
    description: `Delete tag`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"tagId":{"type":"string","description":"Tag Id"}},"required":["Version","locationId","tagId"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}/tags/{tagId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"tagId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["task-search", {
    name: "task-search",
    description: `Task Search`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"contactId":{"description":"Contact Ids","type":"array","items":{"type":"string"}},"completed":{"type":"boolean","description":"Task Completed Or Pending"},"assignedTo":{"description":"Assigned User Ids","type":"array","items":{"type":"string"}},"query":{"type":"string","description":"Search Value"},"limit":{"type":"number","description":"Limit To Api","default":25},"skip":{"type":"number","description":"Number Of Tasks To Skip","default":0},"businessId":{"type":"string","description":"Bussiness Id"}},"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/tasks/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/tasks.readonly"]}]
  }],
  ["get-recurring-task-by-id", {
    name: "get-recurring-task-by-id",
    description: `Get Recurring Task By Id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Recurring Task Id"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","id","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/recurring-tasks/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-recurring-task", {
    name: "update-recurring-task",
    description: `Update Recurring Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Recurring Task Id"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Name of the task"},"description":{"type":"string","description":"Description of the task"},"contactIds":{"description":"Contact Id","type":"array","items":{"type":"string"}},"owners":{"description":"Assigned To","type":"array","items":{"type":"string"}},"rruleOptions":{"description":"Recurring rules","allOf":[{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly"],"example":"monthly"},"interval":{"type":"number","example":2},"startDate":{"type":"string","example":"2021-09-30T00:00:00.000Z","description":"Start Date"},"endDate":{"type":"string","example":"2021-09-30T00:00:00.000Z","description":"End Date"},"dayOfMonth":{"type":"number","description":"1, 2, 3, ..., 27, 31","example":15},"dayOfWeek":{"type":"string","enum":["MO","TU","WE","TH","FR","SA","SU"],"example":"MO"},"monthOfYear":{"type":"number","description":"1, 2, ....., 11, 12","example":1},"count":{"type":"number","description":"Max number of task executions","example":10},"createTaskIfOverDue":{"type":"boolean","example":true,"description":"Create Task If Over Due"},"dueAfterSeconds":{"type":"number","example":23404000,"description":"Due after seconds"}},"required":["intervalType","interval","startDate","dueAfterSeconds"]}]},"ignoreTaskCreation":{"type":"boolean","description":"Create initial task or not"}},"description":"The JSON request body."}},"required":["Version","id","locationId","requestBody"]},
    method: "put",
    pathTemplate: "/locations/{locationId}/recurring-tasks/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-recurring-task", {
    name: "delete-recurring-task",
    description: `Delete Recurring Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Recurring Task Id"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","id","locationId"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}/recurring-tasks/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["create-recurring-task", {
    name: "create-recurring-task",
    description: `Create Recurring Task`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Name of the task"},"description":{"type":"string","description":"Description of the task"},"contactIds":{"description":"Contact Id","type":"array","items":{"type":"string"}},"owners":{"description":"Assigned To","type":"array","items":{"type":"string"}},"rruleOptions":{"description":"Recurring rules","allOf":[{"type":"object","properties":{"intervalType":{"type":"string","enum":["yearly","monthly","weekly","daily","hourly"],"example":"monthly"},"interval":{"type":"number","example":2},"startDate":{"type":"string","example":"2021-09-30T00:00:00.000Z","description":"Start Date"},"endDate":{"type":"string","example":"2021-09-30T00:00:00.000Z","description":"End Date"},"dayOfMonth":{"type":"number","description":"1, 2, 3, ..., 27, 31","example":15},"dayOfWeek":{"type":"string","enum":["MO","TU","WE","TH","FR","SA","SU"],"example":"MO"},"monthOfYear":{"type":"number","description":"1, 2, ....., 11, 12","example":1},"count":{"type":"number","description":"Max number of task executions","example":10},"createTaskIfOverDue":{"type":"boolean","example":true,"description":"Create Task If Over Due"},"dueAfterSeconds":{"type":"number","example":23404000,"description":"Due after seconds"}},"required":["intervalType","interval","startDate","dueAfterSeconds"]}]},"ignoreTaskCreation":{"type":"boolean","description":"Create initial task or not"}},"required":["title","rruleOptions"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/recurring-tasks",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-custom-fields", {
    name: "get-custom-fields",
    description: `Get Custom Fields`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"model":{"enum":["contact","opportunity","all"],"type":"string","description":"Model of the custom field you want to retrieve"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/customFields",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"model","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customFields.readonly"]}]
  }],
  ["create-custom-field", {
    name: "create-custom-field",
    description: `Create Custom Field`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"dataType":{"type":"string"},"placeholder":{"type":"string"},"acceptedFormat":{"type":"array","items":{"type":"string"}},"isMultipleFile":{"type":"boolean"},"maxNumberOfFiles":{"type":"number"},"textBoxListOptions":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}},{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}}]}},"position":{"type":"number","default":0},"model":{"type":"string","description":"Model of the custom field you want to create","enum":["contact","opportunity"]}},"required":["name","dataType"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/customFields",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["get-custom-field", {
    name: "get-custom-field",
    description: `Get Custom Field`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Field Id or Field Key (e.g. \"contact.first_name\" or \"opportunity.pipeline_id\")"}},"required":["Version","locationId","id"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/customFields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-custom-field-2", {
    name: "update-custom-field-2",
    description: `Update Custom Field`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Field Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"placeholder":{"type":"string"},"acceptedFormat":{"type":"array","items":{"type":"string"}},"isMultipleFile":{"type":"boolean"},"maxNumberOfFiles":{"type":"number"},"textBoxListOptions":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}},{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}}]}},"position":{"type":"number","default":0},"model":{"type":"string","description":"Model of the custom field you want to update","enum":["contact","opportunity"]}},"required":["name"],"description":"The JSON request body."}},"required":["Version","locationId","id","requestBody"]},
    method: "put",
    pathTemplate: "/locations/{locationId}/customFields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-custom-field-2", {
    name: "delete-custom-field-2",
    description: `Delete Custom Field`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Field Id"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}/customFields/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["upload-file-customFields", {
    name: "upload-file-customFields",
    description: `Uploads File to customFields`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"string","description":"Request body (content type: multipart/form-data)"}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/customFields/upload",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "multipart/form-data",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["get-custom-values", {
    name: "get-custom-values",
    description: `Get Custom Values`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/customValues",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/customValues.readonly"]}]
  }],
  ["create-custom-value", {
    name: "create-custom-value",
    description: `Create Custom Value`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"value":{"type":"string"}},"required":["name","value"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/locations/{locationId}/customValues",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customValues.write"]}]
  }],
  ["get-custom-value", {
    name: "get-custom-value",
    description: `Get Custom Value`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Value Id"}},"required":["Version","locationId","id"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/customValues/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-custom-value", {
    name: "update-custom-value",
    description: `Update Custom Value`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Value Id"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"value":{"type":"string"}},"required":["name","value"],"description":"The JSON request body."}},"required":["Version","locationId","id","requestBody"]},
    method: "put",
    pathTemplate: "/locations/{locationId}/customValues/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-custom-value", {
    name: "delete-custom-value",
    description: `Delete Custom Value`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Custom Value Id"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}/customValues/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-timezones", {
    name: "get-timezones",
    description: `Fetch the available timezones`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/timezones",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations.readonly"]},{"Location-Access":["locations.readonly"]}]
  }],
  ["GET-all-or-email-sms-templates", {
    name: "GET-all-or-email-sms-templates",
    description: `GET all or email/sms templates`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"deleted":{"default":false,"type":"boolean"},"skip":{"default":"0","type":"string"},"limit":{"default":"25","type":"string"},"type":{"enum":["sms","email","whatsapp"],"type":"string"},"originId":{"type":"string","description":"Origin Id"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","originId","locationId"]},
    method: "get",
    pathTemplate: "/locations/{locationId}/templates",
    executionParameters: [{"name":"Version","in":"header"},{"name":"deleted","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"type","in":"query"},{"name":"originId","in":"query"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["locations/templates.readonly"]}]
  }],
  ["DELETE-an-email-sms-template", {
    name: "DELETE-an-email-sms-template",
    description: `DELETE an email/sms template`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Template Id"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/locations/{locationId}/templates/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["getCharges", {
    name: "getCharges",
    description: `Get all wallet charges`,
    inputSchema: {"type":"object","properties":{"meterId":{"type":"string","description":"Billing Meter ID (you can find this on your app's pricing page on the developer portal)"},"eventId":{"type":"string","description":"Event ID / Transaction ID"},"userId":{"type":"string","description":"Filter results by User ID that your server passed via API when the charge was created"},"startDate":{"type":"string","description":"Filter results AFTER a specific date. Use this in combination with endDate to filter results in a specific time window."},"endDate":{"type":"string","description":"Filter results BEFORE a specific date. Use this in combination with startDate to filter results in a specific time window."},"skip":{"type":"number","description":"Number of records to skip"},"limit":{"type":"number","description":"Maximum number of records to return"}}},
    method: "get",
    pathTemplate: "/marketplace/billing/charges",
    executionParameters: [{"name":"meterId","in":"query"},{"name":"eventId","in":"query"},{"name":"userId","in":"query"},{"name":"startDate","in":"query"},{"name":"endDate","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access-Only":["charges.readonly"]}]
  }],
  ["charge", {
    name: "charge",
    description: `Create a new wallet charge`,
    inputSchema: {"type":"object","properties":{"requestBody":{"type":"object","properties":{"appId":{"type":"string","description":"App ID of the App"},"meterId":{"type":"string","description":"Billing Meter ID (you can find this on your app's pricing page)"},"eventId":{"type":"string","description":"Event ID / Transaction ID on your server's side. This will help you maintain the reference of the event/transaction on your end that you charged the customer for."},"userId":{"type":"string","description":"User ID"},"locationId":{"type":"string","description":"ID of the Sub-Account to be charged"},"companyId":{"type":"string","description":"ID of the Agency the Sub-account belongs to"},"description":{"type":"string","description":"Description of the charge"},"price":{"type":"number","description":"Price per unit to charge"},"units":{"type":"string","description":"Number of units to charge"},"eventTime":{"type":"string","description":"The timestamp when the event/transaction was performed. If blank, the billing timestamp will be set as the event time. ISO8601 Format."}},"required":["appId","meterId","eventId","locationId","companyId","description","units"],"description":"The JSON request body."}},"required":["requestBody"]},
    method: "post",
    pathTemplate: "/marketplace/billing/charges",
    executionParameters: [],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access-Only":["charges.write"]}]
  }],
  ["getSpecificCharge", {
    name: "getSpecificCharge",
    description: `Get specific wallet charge details`,
    inputSchema: {"type":"object","properties":{"chargeId":{"type":"string","description":"ID of the charge to retrieve"}},"required":["chargeId"]},
    method: "get",
    pathTemplate: "/marketplace/billing/charges/{chargeId}",
    executionParameters: [{"name":"chargeId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access-Only":["charges.readonly"]}]
  }],
  ["deleteCharge", {
    name: "deleteCharge",
    description: `Delete a wallet charge`,
    inputSchema: {"type":"object","properties":{"chargeId":{"type":"string","description":"ID of the charge to delete"}},"required":["chargeId"]},
    method: "delete",
    pathTemplate: "/marketplace/billing/charges/{chargeId}",
    executionParameters: [{"name":"chargeId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access-Only":["charges.write"]}]
  }],
  ["hasFunds", {
    name: "hasFunds",
    description: `Check if account has sufficient funds`,
    inputSchema: {"type":"object","properties":{}},
    method: "get",
    pathTemplate: "/marketplace/billing/charges/has-funds",
    executionParameters: [],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access-Only":["charges.readonly"]}]
  }],
  ["get-installer-details", {
    name: "get-installer-details",
    description: `Fetches installer details for the authenticated user. This endpoint returns information about the company, location, user, and installation details associated with the current OAuth token.`,
    inputSchema: {"type":"object","properties":{"appId":{"type":"string","description":"ID of the app to get installer details"}},"required":["appId"]},
    method: "get",
    pathTemplate: "/marketplace/app/{appId}/installations",
    executionParameters: [{"name":"appId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["uninstall-application", {
    name: "uninstall-application",
    description: `Uninstalls an application from your company or a specific location. This will remove the application\`s access and stop all its functionalities`,
    inputSchema: {"type":"object","properties":{"appId":{"type":"string","description":"The application id which is to be uninstalled."},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"companyId":{"type":"string","description":"The company id from which the application is to be uninstalled. If you pass agency token, then companyId is required. It will uninstall application from agency as well as all sub-accounts."},"locationId":{"type":"string","description":"The location id from which the application is to be uninstalled. If you pass location token, then locationId is required. It will uninstall application from that location only."},"reason":{"type":"string","description":"The reason for uninstalling the application. Reason is required if you are uninstalling the application as a developer."}},"description":"The JSON request body."}},"required":["appId","Version","requestBody"]},
    method: "delete",
    pathTemplate: "/marketplace/app/{appId}/installations",
    executionParameters: [{"name":"appId","in":"path"},{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access-Only":[]},{"Agency-Access":[]}]
  }],
  ["fetch-media-content", {
    name: "fetch-media-content",
    description: `Fetches list of files and folders from the media library`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"offset":{"type":"string","description":"Number of files to skip in listing"},"limit":{"type":"string","description":"Number of files to show in the listing"},"sortBy":{"type":"string","description":"Field to sorting the file listing by"},"sortOrder":{"type":"string","description":"Direction in which file needs to be sorted"},"type":{"type":"string","description":"Type"},"query":{"type":"string","description":"Query text"},"altType":{"type":"string","enum":["location"],"description":"AltType"},"altId":{"type":"string","description":"location Id"},"parentId":{"type":"string","description":"parent id or folder id"},"fetchAll":{"type":"string","description":"Fetch all files or folders"}},"required":["Version","sortBy","sortOrder","type","altType","altId"]},
    method: "get",
    pathTemplate: "/medias/files",
    executionParameters: [{"name":"Version","in":"header"},{"name":"offset","in":"query"},{"name":"limit","in":"query"},{"name":"sortBy","in":"query"},{"name":"sortOrder","in":"query"},{"name":"type","in":"query"},{"name":"query","in":"query"},{"name":"altType","in":"query"},{"name":"altId","in":"query"},{"name":"parentId","in":"query"},{"name":"fetchAll","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["medias.readonly"]}]
  }],
  ["upload-media-content", {
    name: "upload-media-content",
    description: `If hosted is set to true then fileUrl is required. Else file is required. If adding a file, maximum allowed is 25 MB`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"string","description":"Request body (content type: multipart/form-data)"}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/medias/upload-file",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "multipart/form-data",
    securityRequirements: [{"Location-Access":["medias.write"]}]
  }],
  ["update-media-object", {
    name: "update-media-object",
    description: `Updates a single file or folder by ID`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Unique identifier of the file or folder to update"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"New name for the file or folder"},"altType":{"type":"string","description":"Type of entity that owns the file or folder","enum":["location"]},"altId":{"type":"string","description":"Location identifier that owns the file or folder"}},"required":["name","altType","altId"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "post",
    pathTemplate: "/medias/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["delete-media-content", {
    name: "delete-media-content",
    description: `Deletes specific file or folder from the media library`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string"},"altType":{"type":"string","enum":["location"],"description":"AltType"},"altId":{"type":"string","description":"location Id"}},"required":["Version","id","altType","altId"]},
    method: "delete",
    pathTemplate: "/medias/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"altType","in":"query"},{"name":"altId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["medias.write"]}]
  }],
  ["create-media-folder", {
    name: "create-media-folder",
    description: `Creates a new folder in the media library`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"Type of entity (location only)","enum":["location"]},"name":{"type":"string","description":"Name of the folder to be created"},"parentId":{"type":"string","description":"ID of the parent folder (optional)"}},"required":["altId","altType","name"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/medias/folder",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["bulk-update-media-objects", {
    name: "bulk-update-media-objects",
    description: `Updates metadata or status of multiple files and folders`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location identifier"},"altType":{"type":"string","description":"Type of entity that owns the files","enum":["location"]},"filesToBeUpdated":{"type":"array","description":"Array of file objects to be updated","items":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier of the file or folder to be updated"},"name":{"type":"string","description":"New name for the file or folder"}},"required":["id"]}}},"required":["altId","altType","filesToBeUpdated"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "put",
    pathTemplate: "/medias/update-files",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["bulk-delete-media-objects", {
    name: "bulk-delete-media-objects",
    description: `Soft-deletes or trashes multiple files and folders in a single request`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"filesToBeDeleted":{"type":"array","description":"Array of file objects to be deleted or trashed","items":{"type":"object","properties":{"_id":{"type":"string","description":"Unique identifier of the file or folder to be deleted"}},"required":["_id"]}},"altType":{"type":"string","description":"Type of entity that owns the files","enum":["location"]},"altId":{"type":"string","description":"Location identifier"},"status":{"type":"string","description":"Status to set for the files (deleted or trashed)","enum":["deleted","trashed"]}},"required":["filesToBeDeleted","altType","altId","status"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "put",
    pathTemplate: "/medias/delete-files",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-access-token", {
    name: "get-access-token",
    description: `Use Access Tokens to access GoHighLevel resources on behalf of an authenticated location/company.`,
    inputSchema: {"type":"object","properties":{"requestBody":{"type":"string","description":"Request body (content type: application/x-www-form-urlencoded)"}},"required":["requestBody"]},
    method: "post",
    pathTemplate: "/oauth/token",
    executionParameters: [],
    requestBodyContentType: "application/x-www-form-urlencoded",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["get-location-access-token", {
    name: "get-location-access-token",
    description: `This API allows you to generate locationAccessToken from AgencyAccessToken`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"string","description":"Request body (content type: application/x-www-form-urlencoded)"}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/oauth/locationToken",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/x-www-form-urlencoded",
    securityRequirements: [{"Agency-Access-Only":["oauth.write"]}]
  }],
  ["get-installed-location", {
    name: "get-installed-location",
    description: `This API allows you fetch location where app is installed upon`,
    inputSchema: {"type":"object","properties":{"skip":{"default":"0","type":"string","description":"Parameter to skip the number installed locations"},"limit":{"default":"20","type":"string","description":"Parameter to limit the number installed locations"},"query":{"type":"string","description":"Parameter to search for the installed location by name"},"isInstalled":{"type":"boolean","description":"Filters out location which are installed for specified app under the specified company"},"companyId":{"type":"string","description":"Parameter to search by the companyId"},"appId":{"type":"string","description":"Parameter to search by the appId"},"versionId":{"type":"string","description":"VersionId of the app"},"onTrial":{"type":"boolean","description":"Filters out locations which are installed for specified app in trial mode"},"planId":{"type":"string","description":"Filters out location which are installed for specified app under the specified planId"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["companyId","appId","Version"]},
    method: "get",
    pathTemplate: "/oauth/installedLocations",
    executionParameters: [{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"query","in":"query"},{"name":"isInstalled","in":"query"},{"name":"companyId","in":"query"},{"name":"appId","in":"query"},{"name":"versionId","in":"query"},{"name":"onTrial","in":"query"},{"name":"planId","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["oauth.readonly"]}]
  }],
  ["get-object-schema-by-key", {
    name: "get-object-schema-by-key",
    description: `Retrieve Object Schema by key or ID. This will return the schema of the custom object, including all its fields and properties. Supported objects include contact, opportunity, business and custom objects.To understand objects and records, please have a look the documentation here : https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"key":{"type":"string","description":"key of the custom or standard object. For custom objects, the key must include the prefix “custom_objects.”. This key can be found on the Object Details page under Settings in the UI."},"locationId":{"type":"string","description":"location id of the sub account"},"fetchProperties":{"type":"string","description":"Fetch Properties , Fetches all the standard / custom fields of the object when set to true"}},"required":["Version","key","locationId"]},
    method: "get",
    pathTemplate: "/objects/{key}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"key","in":"path"},{"name":"locationId","in":"query"},{"name":"fetchProperties","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["objects/schema.readonly"]}]
  }],
  ["update-custom-object", {
    name: "update-custom-object",
    description: `Update Custom Object Schema  or standard object's like contact, opportunity, business searchable fields. To understand objects and records, please have a look at the documentation here : https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"key":{"type":"string","description":"key of the custom or standard object. For custom objects, the key must include the prefix “custom_objects.”. This key can be found on the Object Details page under Settings in the UI."},"requestBody":{"type":"object","properties":{"labels":{"description":"This is how your custom object will  be  displayed","allOf":[{"type":"object","properties":{"singular":{"type":"string","example":"Car","description":"Singular name of the custom object"},"plural":{"type":"string","example":"Cars","description":"Plural name of the custom object"}}}]},"description":{"type":["string","null"],"description":"Pet Object`s description"},"locationId":{"type":"string","description":"location id"},"searchableProperties":{"description":"Searchable Fields: Provide the field key of your object that you want to search on, using the format (custom_object.<object_name>.<field_key>).","type":"array","items":{"type":"string"}}},"required":["locationId","searchableProperties"],"description":"The JSON request body."}},"required":["Version","key","requestBody"]},
    method: "put",
    pathTemplate: "/objects/{key}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"key","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["objects/schema.write"]}]
  }],
  ["get-record-by-id", {
    name: "get-record-by-id",
    description: `Allows you to get a Standard Object like business and custom object record by Id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"schemaKey":{"type":"string","description":"The key of the Custom Object / Standard Object Schema. For custom objects, the key must include the “custom_objects.” prefix, while standard objects use their respective object keys. This information is available on the Custom Objects Details page under Settings."},"id":{"type":"string","description":"id of the record to be updated. Available on the Record details page under the 3 dots or in the url"}},"required":["Version","schemaKey","id"]},
    method: "get",
    pathTemplate: "/objects/{schemaKey}/records/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"schemaKey","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["update-object-record", {
    name: "update-object-record",
    description: `Update a Custom Object Record by Id. Supported Objects are business and custom objects. Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0/87cpx-376296`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"schemaKey":{"type":"string","description":"The key of the Custom Object / Standard Object Schema. For custom objects, the key must include the “custom_objects.” prefix, while standard objects use their respective object keys. This information is available on the Custom Objects Details page under Settings."},"id":{"type":"string","description":"id of the record to be updated. Available on the Record details page under the 3 dots or in the url"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{},"description":"The JSON request body."}},"required":["Version","schemaKey","id","locationId","requestBody"]},
    method: "put",
    pathTemplate: "/objects/{schemaKey}/records/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"schemaKey","in":"path"},{"name":"id","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-object-record", {
    name: "delete-object-record",
    description: `Delete Record By Id . Supported Objects are business and custom objects.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"schemaKey":{"type":"string","description":"The key of the Custom Object / Standard Object Schema. For custom objects, the key must include the “custom_objects.” prefix, while standard objects use their respective object keys. This information is available on the Custom Objects Details page under Settings."},"id":{"type":"string","description":"id of the record to be updated. Available on the Record details page under the 3 dots or in the url"}},"required":["Version","schemaKey","id"]},
    method: "delete",
    pathTemplate: "/objects/{schemaKey}/records/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"schemaKey","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["create-object-record", {
    name: "create-object-record",
    description: `Create a Custom Object Record. Supported Objects business and custom objects. Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0/87cpx-376296`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"schemaKey":{"type":"string","description":"The key of the Custom Object / Standard Object Schema. For custom objects, the key must include the “custom_objects.” prefix, while standard objects use their respective object keys. This information is available on the Custom Objects Details page under Settings."},"requestBody":{"type":"object","properties":{},"description":"The JSON request body."}},"required":["Version","schemaKey","requestBody"]},
    method: "post",
    pathTemplate: "/objects/{schemaKey}/records",
    executionParameters: [{"name":"Version","in":"header"},{"name":"schemaKey","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["objects/record.write"]}]
  }],
  ["search-object-records", {
    name: "search-object-records",
    description: `Supported Objects are custom objects and standard objects like "business". Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0/87cpx-379336`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"schemaKey":{"type":"string","description":"custom object key"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location Id"},"page":{"type":"number","description":"Page"},"pageLimit":{"type":"number","description":"Page Limit"},"query":{"type":"string","description":"Pass this query parameter to search using your searchable properties. For example, if you have a custom object called “Pets” and have configured “name” as a searchable property, you can pass name:Buddy to search for pets with the name “Buddy.”"},"searchAfter":{"type":"array","items":{"type":"string"}}},"required":["locationId","page","pageLimit","query","searchAfter"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/objects/{schemaKey}/records/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"schemaKey","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["objects/record.readonly"]}]
  }],
  ["search-opportunity", {
    name: "search-opportunity",
    description: `Search Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"q":{"type":"string"},"location_id":{"type":"string","description":"Location Id"},"pipeline_id":{"type":"string","description":"Pipeline Id"},"pipeline_stage_id":{"type":"string","description":"stage Id"},"contact_id":{"type":"string","description":"Contact Id"},"status":{"enum":["open","won","lost","abandoned","all"],"type":"string"},"assigned_to":{"type":"string"},"campaignId":{"type":"string","description":"Campaign Id"},"id":{"type":"string","description":"Opportunity Id"},"order":{"type":"string"},"endDate":{"type":"string","description":"End date"},"startAfter":{"type":"string","description":"Start After"},"startAfterId":{"type":"string","description":"Start After Id"},"date":{"type":"string","description":"Start date"},"country":{"type":"string"},"page":{"default":1,"type":"number"},"limit":{"default":20,"type":"number","description":"Limit Per Page records count. will allow maximum up to 100 and default will be 20"},"getTasks":{"type":"boolean","description":"get Tasks in contact"},"getNotes":{"type":"boolean","description":"get Notes in contact"},"getCalendarEvents":{"type":"boolean","description":"get Calender event in contact"}},"required":["Version","location_id"]},
    method: "get",
    pathTemplate: "/opportunities/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"q","in":"query"},{"name":"location_id","in":"query"},{"name":"pipeline_id","in":"query"},{"name":"pipeline_stage_id","in":"query"},{"name":"contact_id","in":"query"},{"name":"status","in":"query"},{"name":"assigned_to","in":"query"},{"name":"campaignId","in":"query"},{"name":"id","in":"query"},{"name":"order","in":"query"},{"name":"endDate","in":"query"},{"name":"startAfter","in":"query"},{"name":"startAfterId","in":"query"},{"name":"date","in":"query"},{"name":"country","in":"query"},{"name":"page","in":"query"},{"name":"limit","in":"query"},{"name":"getTasks","in":"query"},{"name":"getNotes","in":"query"},{"name":"getCalendarEvents","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["opportunities.readonly"]}]
  }],
  ["get-pipelines", {
    name: "get-pipelines",
    description: `Get Pipelines`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/opportunities/pipelines",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["opportunities.readonly"]}]
  }],
  ["get-opportunity", {
    name: "get-opportunity",
    description: `Get Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"}},"required":["Version","id"]},
    method: "get",
    pathTemplate: "/opportunities/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["opportunities.readonly"]}]
  }],
  ["update-opportunity", {
    name: "update-opportunity",
    description: `Update Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"},"requestBody":{"type":"object","properties":{"pipelineId":{"type":"string","description":"pipeline Id"},"name":{"type":"string"},"pipelineStageId":{"type":"string"},"status":{"type":"string","enum":["open","won","lost","abandoned","all"]},"monetaryValue":{"type":"number"},"assignedTo":{"type":"string"},"customFields":{"type":"array","description":"Update custom fields to opportunities.","items":{"anyOf":[{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ","description":"Pass either `id` or `key` of custom field"},"key":{"type":"string","example":"my_custom_field","description":"Pass either `id` or `key` of custom field"},"field_value":{"type":"string","example":"9039160788"}}},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":{}}},"required":["id"]}]}}},"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "put",
    pathTemplate: "/opportunities/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["delete-opportunity", {
    name: "delete-opportunity",
    description: `Delete Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"}},"required":["Version","id"]},
    method: "delete",
    pathTemplate: "/opportunities/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["update-opportunity-status", {
    name: "update-opportunity-status",
    description: `Update Opportunity Status`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"},"requestBody":{"type":"object","properties":{"status":{"type":"string","enum":["open","won","lost","abandoned","all"]}},"required":["status"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "put",
    pathTemplate: "/opportunities/{id}/status",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["Upsert-opportunity", {
    name: "Upsert-opportunity",
    description: `Upsert Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"pipelineId":{"type":"string","description":"pipeline Id"},"locationId":{"type":"string","description":"locationId"},"contactId":{"type":"string","description":"contactId"},"name":{"type":"string","description":"name"},"status":{"type":"string","enum":["open","won","lost","abandoned","all"]},"pipelineStageId":{"type":"string"},"monetaryValue":{"type":"number"},"assignedTo":{"type":"string"}},"required":["pipelineId","locationId","contactId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/opportunities/upsert",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["add-followers-opportunity", {
    name: "add-followers-opportunity",
    description: `Add Followers`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"},"requestBody":{"type":"object","properties":{"followers":{"type":"array","items":{"type":"string"}}},"required":["followers"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "post",
    pathTemplate: "/opportunities/{id}/followers",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["remove-followers-opportunity", {
    name: "remove-followers-opportunity",
    description: `Remove Followers`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Opportunity Id"},"requestBody":{"type":"object","properties":{"followers":{"type":"array","items":{"type":"string"}}},"required":["followers"],"description":"The JSON request body."}},"required":["Version","id","requestBody"]},
    method: "delete",
    pathTemplate: "/opportunities/{id}/followers",
    executionParameters: [{"name":"Version","in":"header"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["list-integration-providers", {
    name: "list-integration-providers",
    description: `The "List White-label Integration Providers" API allows to retrieve a paginated list of integration providers. Customize your results by filtering whitelabel integration providers(which are built directly on top of Authorize.net or NMI) based on name or paginate through the list using the provided query parameters. This endpoint provides a straightforward way to explore and retrieve integration provider information.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/integrations/provider/whitelabel",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/integration.readonly"]}]
  }],
  ["create-integration-provider", {
    name: "create-integration-provider",
    description: `The "Create White-label Integration Provider" API allows adding a new payment provider integration to the system which is built on top of Authorize.net or NMI. Use this endpoint to create a integration provider with the specified details. Ensure that the required information is provided in the request payload. This endpoint can be only invoked using marketplace-app token`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"uniqueName":{"type":"string","description":"A unique name given to the integration provider, uniqueName must start and end with a character. Only lowercase characters and hyphens (-) are supported"},"title":{"type":"string","description":"The title or name of the integration provider."},"provider":{"type":"string","description":"The type of payment provider associated with the integration provider.","enum":["authorize-net","nmi"]},"description":{"type":"string","description":"A brief description providing additional information about the integration provider."},"imageUrl":{"type":"string","description":"The URL to an image representing the integration provider. The imageUrl should start with \"https://\" and ensure that this URL is publicly accessible."}},"required":["altId","altType","uniqueName","title","provider","description","imageUrl"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/payments/integrations/provider/whitelabel",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/integration.write"]}]
  }],
  ["list-orders", {
    name: "list-orders",
    description: `The "List Orders" API allows to retrieve a paginated list of orders. Customize your results by filtering orders based on name, alt type, order status, payment mode, date range, type of source, contact, funnel products or paginate through the list using the provided query parameters. This endpoint provides a straightforward way to explore and retrieve order information.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"LocationId is the id of the sub-account."},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."},"altType":{"type":"string","description":"AltType is the type of identifier."},"status":{"type":"string","description":"Order status."},"paymentMode":{"type":"string","description":"Mode of payment."},"startAt":{"type":"string","description":"Starting interval of orders."},"endAt":{"type":"string","description":"Closing interval of orders."},"search":{"type":"string","description":"The name of the order for searching."},"contactId":{"type":"string","description":"Contact id for filtering of orders."},"funnelProductIds":{"type":"string","description":"Funnel product ids separated by comma."},"limit":{"default":10,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/orders",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"status","in":"query"},{"name":"paymentMode","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"search","in":"query"},{"name":"contactId","in":"query"},{"name":"funnelProductIds","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/orders.readonly"]}]
  }],
  ["get-order-by-id", {
    name: "get-order-by-id",
    description: `The "Get Order by ID" API allows to retrieve information for a specific order using its unique identifier. Use this endpoint to fetch details for a single order based on the provided order ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"orderId":{"type":"string","description":"ID of the order that needs to be returned"},"locationId":{"type":"string","description":"LocationId is the id of the sub-account."},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."}},"required":["Version","orderId","altId"]},
    method: "get",
    pathTemplate: "/payments/orders/{orderId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"orderId","in":"path"},{"name":"locationId","in":"query"},{"name":"altId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/orders.readonly"]}]
  }],
  ["record-order-payment", {
    name: "record-order-payment",
    description: `The "Record Order Payment" API allows to record a payment for an order. Use this endpoint to record payment for an order and update the order status to "Paid".`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"orderId":{"type":"string","description":"MongoDB Order ID"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"mode":{"type":"string","description":"manual payment method","enum":["cash","card","cheque","bank_transfer","other"]},"card":{"description":"Details of Card if used for payment","allOf":[{"type":"object","properties":{"type":{"type":"string","enum":["visa","mastercard","other"],"example":"mastercard"},"last4":{"type":"string","description":"Last 4 digit of the card","example":"1234"}},"required":["type","last4"]}]},"cheque":{"description":"Details of the Cheque if used for payment","allOf":[{"type":"object","properties":{"number":{"type":"string","description":"check number","example":"129-129-129-912"}},"required":["number"]}]},"notes":{"type":"string","description":"Any note to be recorded with the transaction"},"amount":{"type":"number","description":"Amount to be paid against the invoice."},"meta":{"type":"object","description":"Meta data to be recorded with the transaction"},"isPartialPayment":{"type":"boolean","description":"Indicates if the order is intended to be a partial payment."}},"required":["altId","altType","mode"],"description":"The JSON request body."}},"required":["Version","orderId","requestBody"]},
    method: "post",
    pathTemplate: "/payments/orders/{orderId}/record-payment",
    executionParameters: [{"name":"Version","in":"header"},{"name":"orderId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/orders.collectPayment"]}]
  }],
  ["post-migrate-order-payment-status", {
    name: "post-migrate-order-payment-status",
    description: `Process to migrate all the older orders and based on the statuses introduce the payment statuses as well`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"LocationId is the id of the sub-account."},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."}},"required":["Version","altId"]},
    method: "post",
    pathTemplate: "/payments/orders/migrate-order-ps",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"altId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["list-order-fulfillment", {
    name: "list-order-fulfillment",
    description: `List all fulfillment history of an order`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"orderId":{"type":"string","description":"ID of the order that needs to be returned"}},"required":["Version","altId","altType","orderId"]},
    method: "get",
    pathTemplate: "/payments/orders/{orderId}/fulfillments",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"orderId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/orders.readonly"]}]
  }],
  ["create-order-fulfillment", {
    name: "create-order-fulfillment",
    description: `The "Order Fulfillment" API facilitates the process of fulfilling an order.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"orderId":{"type":"string","description":"ID of the order that needs to be returned"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"trackings":{"description":"Fulfillment tracking information","type":"array","items":{"type":"object","properties":{"trackingNumber":{"type":"string","description":"Tracking number provided by the shipping carrier"},"shippingCarrier":{"type":"string","description":"Shipping carrier name"},"trackingUrl":{"type":"string","description":"Tracking URL"}}}},"items":{"description":"Fulfilled items","type":"array","items":{"type":"object","properties":{"priceId":{"type":"string","description":"The id of product price"},"qty":{"type":"number","description":"The no of quantity of the item"}},"required":["priceId","qty"]}},"notifyCustomer":{"type":"boolean","description":"Need to send a notification to customer"}},"required":["altId","altType","trackings","items","notifyCustomer"],"description":"The JSON request body."}},"required":["Version","orderId","requestBody"]},
    method: "post",
    pathTemplate: "/payments/orders/{orderId}/fulfillments",
    executionParameters: [{"name":"Version","in":"header"},{"name":"orderId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/orders.write"]}]
  }],
  ["list-order-notes", {
    name: "list-order-notes",
    description: `List all notes of an order`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"orderId":{"type":"string","description":"ID of the order that needs to be returned"}},"required":["Version","altId","altType","orderId"]},
    method: "get",
    pathTemplate: "/payments/orders/{orderId}/notes",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"orderId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["list-transactions", {
    name: "list-transactions",
    description: `The "List Transactions" API allows to retrieve a paginated list of transactions. Customize your results by filtering transactions based on name, alt type, transaction status, payment mode, date range, type of source, contact, subscription id, entity id or paginate through the list using the provided query parameters. This endpoint provides a straightforward way to explore and retrieve transaction information.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"LocationId is the id of the sub-account."},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."},"altType":{"type":"string","description":"AltType is the type of identifier."},"paymentMode":{"type":"string","description":"Mode of payment."},"startAt":{"type":"string","description":"Starting interval of transactions."},"endAt":{"type":"string","description":"Closing interval of transactions."},"entitySourceType":{"type":"string","description":"Source of the transactions."},"entitySourceSubType":{"type":"string","description":"Source sub-type of the transactions."},"search":{"type":"string","description":"The name of the transaction for searching."},"subscriptionId":{"type":"string","description":"Subscription id for filtering of transactions."},"entityId":{"type":"string","description":"Entity id for filtering of transactions."},"contactId":{"type":"string","description":"Contact id for filtering of transactions."},"limit":{"default":10,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/transactions",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"paymentMode","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"entitySourceType","in":"query"},{"name":"entitySourceSubType","in":"query"},{"name":"search","in":"query"},{"name":"subscriptionId","in":"query"},{"name":"entityId","in":"query"},{"name":"contactId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/transactions.readonly"]}]
  }],
  ["get-transaction-by-id", {
    name: "get-transaction-by-id",
    description: `The "Get Transaction by ID" API allows to retrieve information for a specific transaction using its unique identifier. Use this endpoint to fetch details for a single transaction based on the provided transaction ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"transactionId":{"type":"string","description":"ID of the transaction that needs to be returned"},"locationId":{"type":"string","description":"LocationId is the id of the sub-account."},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."},"altType":{"type":"string","description":"AltType is the type of identifier."}},"required":["Version","transactionId","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/transactions/{transactionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"transactionId","in":"path"},{"name":"locationId","in":"query"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/transactions.readonly"]}]
  }],
  ["list-subscriptions", {
    name: "list-subscriptions",
    description: `The "List Subscriptions" API allows to retrieve a paginated list of subscriptions. Customize your results by filtering subscriptions based on name, alt type, subscription status, payment mode, date range, type of source, contact, subscription id, entity id, contact or paginate through the list using the provided query parameters. This endpoint provides a straightforward way to explore and retrieve subscription information.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."},"altType":{"enum":["location"],"type":"string","description":"AltType is the type of identifier."},"entityId":{"type":"string","description":"Entity id for filtering of subscriptions."},"paymentMode":{"type":"string","description":"Mode of payment."},"startAt":{"type":"string","description":"Starting interval of subscriptions."},"endAt":{"type":"string","description":"Closing interval of subscriptions."},"entitySourceType":{"type":"string","description":"Source of the subscriptions."},"search":{"type":"string","description":"The name of the subscription for searching."},"contactId":{"type":"string","description":"Contact ID for the subscription"},"id":{"type":"string","description":"Subscription id for filtering of subscriptions."},"limit":{"default":10,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/subscriptions",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"entityId","in":"query"},{"name":"paymentMode","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"entitySourceType","in":"query"},{"name":"search","in":"query"},{"name":"contactId","in":"query"},{"name":"id","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/subscriptions.readonly"]}]
  }],
  ["get-subscription-by-id", {
    name: "get-subscription-by-id",
    description: `The "Get Subscription by ID" API allows to retrieve information for a specific subscription using its unique identifier. Use this endpoint to fetch details for a single subscription based on the provided subscription ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"subscriptionId":{"type":"string","description":"ID of the subscription that needs to be returned"},"altId":{"type":"string","description":"AltId is the unique identifier e.g: location id."},"altType":{"enum":["location"],"type":"string","description":"AltType is the type of identifier."}},"required":["Version","subscriptionId","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/subscriptions/{subscriptionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"subscriptionId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/subscriptions.readonly"]}]
  }],
  ["list-coupons", {
    name: "list-coupons",
    description: `The "List Coupons" API allows you to retrieve a list of all coupons available in your location. Use this endpoint to view all promotional offers and special discounts for your customers.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"limit":{"default":100,"type":"number","description":"Maximum number of coupons to return"},"offset":{"default":0,"type":"number","description":"Number of coupons to skip for pagination"},"status":{"enum":["scheduled","active","expired"],"type":"string","description":"Filter coupons by status"},"search":{"type":"string","description":"Search term to filter coupons by name or code"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/payments/coupon/list",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"status","in":"query"},{"name":"search","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/coupons.readonly"]}]
  }],
  ["get-coupon", {
    name: "get-coupon",
    description: `The "Get Coupon Details" API enables you to retrieve comprehensive information about a specific coupon using either its unique identifier or promotional code. Use this endpoint to view coupon parameters, usage statistics, validity periods, and other promotional details.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"id":{"type":"string","description":"Coupon id"},"code":{"type":"string","description":"Coupon code"}},"required":["Version","altId","altType","id","code"]},
    method: "get",
    pathTemplate: "/payments/coupon",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"id","in":"query"},{"name":"code","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/coupons.readonly"]}]
  }],
  ["update-coupon", {
    name: "update-coupon",
    description: `The "Update Coupon" API enables you to modify existing coupon details such as discount values, validity periods, usage limits, and other promotional parameters. Use this endpoint to adjust or extend promotional offers for your customers.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string","description":"Coupon Name"},"code":{"type":"string","description":"Coupon Code"},"discountType":{"type":"string","description":"Discount Type","enum":["percentage","amount"]},"discountValue":{"type":"number","description":"Discount Value"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format"},"endDate":{"type":"string","description":"End date in YYYY-MM-DDTHH:mm:ssZ format"},"usageLimit":{"type":"number","description":"Max number of times coupon can be used"},"productIds":{"description":"Product Ids","type":"array","items":{"type":"string"}},"applyToFuturePayments":{"type":"boolean","default":true,"description":"Is Coupon applicable on upcoming subscription transactions"},"applyToFuturePaymentsConfig":{"default":{"type":"forever"},"description":"If coupon is applicable on upcoming subscription transactions, how many months should it be applicable for a subscription","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Type of the config","example":"forever | fixed","enum":["forever","fixed"]},"duration":{"type":"number","description":"Duration the coupon to be applied in a subscription","example":5},"durationType":{"type":"string","description":"Type of the duration","example":"months","enum":["months"]}},"required":["type","duration","durationType"]}]},"limitPerCustomer":{"type":"boolean","default":false,"description":"Limits whether a coupon can be redeemed only once per customer."},"id":{"type":"string","description":"Coupon Id"}},"required":["altId","altType","name","code","discountType","discountValue","startDate","id"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "put",
    pathTemplate: "/payments/coupon",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/coupons.write"]}]
  }],
  ["create-coupon", {
    name: "create-coupon",
    description: `The "Create Coupon" API allows you to create a new promotional coupon with customizable parameters such as discount amount, validity period, usage limits, and applicable products. Use this endpoint to set up promotional offers and special discounts for your customers.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string","description":"Coupon Name"},"code":{"type":"string","description":"Coupon Code"},"discountType":{"type":"string","description":"Discount Type","enum":["percentage","amount"]},"discountValue":{"type":"number","description":"Discount Value"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format"},"endDate":{"type":"string","description":"End date in YYYY-MM-DDTHH:mm:ssZ format"},"usageLimit":{"type":"number","description":"Max number of times coupon can be used"},"productIds":{"description":"Product Ids","type":"array","items":{"type":"string"}},"applyToFuturePayments":{"type":"boolean","default":true,"description":"Is Coupon applicable on upcoming subscription transactions"},"applyToFuturePaymentsConfig":{"default":{"type":"forever"},"description":"If coupon is applicable on upcoming subscription transactions, how many months should it be applicable for a subscription","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Type of the config","example":"forever | fixed","enum":["forever","fixed"]},"duration":{"type":"number","description":"Duration the coupon to be applied in a subscription","example":5},"durationType":{"type":"string","description":"Type of the duration","example":"months","enum":["months"]}},"required":["type","duration","durationType"]}]},"limitPerCustomer":{"type":"boolean","default":false,"description":"Limits whether a coupon can be redeemed only once per customer."}},"required":["altId","altType","name","code","discountType","discountValue","startDate"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/payments/coupon",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/coupons.write"]}]
  }],
  ["delete-coupon", {
    name: "delete-coupon",
    description: `The "Delete Coupon" API allows you to permanently remove a coupon from your system using its unique identifier. Use this endpoint to discontinue promotional offers or clean up unused coupons. Note that this action cannot be undone.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"id":{"type":"string","description":"Coupon Id"}},"required":["altId","altType","id"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "delete",
    pathTemplate: "/payments/coupon",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/coupons.write"]}]
  }],
  ["create-integration", {
    name: "create-integration",
    description: `API to create a new association for an app and location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location id"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name of the custom provider"},"description":{"type":"string","description":"Description of payment gateway. Shown on the payments integrations page as subtext"},"paymentsUrl":{"type":"string","description":"This url will be loaded in iFrame to start a payment session."},"queryUrl":{"type":"string","description":"The url used for querying payments related events. Ex. verify, refund, subscription etc."},"imageUrl":{"type":"string","description":"Public image url for logo of the payment gateway displayed on the payments integrations page."},"supportsSubscriptionSchedule":{"type":"boolean","description":"Whether the config supports subscription schedule or not. true represents config supports subscription schedule"}},"required":["name","description","paymentsUrl","queryUrl","imageUrl","supportsSubscriptionSchedule"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/payments/custom-provider/provider",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/custom-provider.write"]}]
  }],
  ["delete-integration", {
    name: "delete-integration",
    description: `API to delete an association for an app and location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location id"}},"required":["Version","locationId"]},
    method: "delete",
    pathTemplate: "/payments/custom-provider/provider",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/custom-provider.write"]}]
  }],
  ["fetch-config", {
    name: "fetch-config",
    description: `API for fetching an existing payment config for given location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/payments/custom-provider/connect",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["payments/custom-provider.readonly"]}]
  }],
  ["create-config", {
    name: "create-config",
    description: `API to create a new payment config for given location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location id"},"requestBody":{"type":"object","properties":{"live":{"description":"Live config containing api-key and publishable key for live payments","allOf":[{"type":"object","properties":{"apiKey":{"type":"string","description":"Api-key for custom payment provider config","example":"y5ZQxryRFXZHvUJZdLeXXXXX"},"publishableKey":{"type":"string","description":"Publishable-key for custom payment provider config","example":"rzp_test_zPRoVMLOa0XXXX"}},"required":["apiKey","publishableKey"]}]},"test":{"description":"Test config containing api-key and publishable-key for test payments","allOf":[{"type":"object","properties":{"apiKey":{"type":"string","description":"Api-key for custom payment provider config","example":"y5ZQxryRFXZHvUJZdLeXXXXX"},"publishableKey":{"type":"string","description":"Publishable-key for custom payment provider config","example":"rzp_test_zPRoVMLOa0XXXX"}},"required":["apiKey","publishableKey"]}]}},"required":["live","test"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/payments/custom-provider/connect",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/custom-provider.write"]}]
  }],
  ["disconnect-config", {
    name: "disconnect-config",
    description: `API to disconnect an existing payment config for given location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location id"},"requestBody":{"type":"object","properties":{"liveMode":{"type":"boolean","description":"Whether the config is for test mode or live mode. true represents config is for live payments"}},"required":["liveMode"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/payments/custom-provider/disconnect",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/custom-provider.write"]}]
  }],
  ["custom-provider-marketplace-app-update-capabilities", {
    name: "custom-provider-marketplace-app-update-capabilities",
    description: `Toggle capabilities for the marketplace app tied to the OAuth client`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"supportsSubscriptionSchedules":{"type":"boolean","description":"Whether the marketplace app supports subscription schedules or not"},"companyId":{"type":"string","description":"Company id. Mandatory if locationId is not provided"},"locationId":{"type":"string","description":"Location / Sub-account id. Mandatory if companyId is not provided"}},"required":["supportsSubscriptionSchedules"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "put",
    pathTemplate: "/payments/custom-provider/capabilities",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["payments/custom-provider.write"]}]
  }],
  ["getNumberPoolList", {
    name: "getNumberPoolList",
    description: `Get list of number pools`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string","description":"Location ID to filter pools"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["Version"]},
    method: "get",
    pathTemplate: "/phone-system/number-pools",
    executionParameters: [{"name":"locationId","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["numberpools.read"]}]
  }],
  ["active-numbers", {
    name: "active-numbers",
    description: `Retrieve a paginated list of active phone numbers for a specific location. Supports filtering, pagination, and optional exclusion of number pool assignments.`,
    inputSchema: {"type":"object","properties":{"locationId":{"type":"string","description":"The unique identifier of the location"},"pageSize":{"type":"number","minimum":1,"maximum":1000,"default":1000,"description":"How many resources to return in each list page. The default is 50, and the maximum is 1000."},"page":{"type":"number","minimum":0,"default":0,"description":"The page index for pagination. The default is 0."},"searchFilter":{"type":"string","description":"Filter numbers by phone number pattern. Supports partial matching (e.g., \"+91\" to find all Indian numbers)."},"skipNumberPool":{"type":"boolean","default":true,"description":"Whether to exclude numbers that are assigned to number pools. Default is true."},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["locationId","Version"]},
    method: "get",
    pathTemplate: "/phone-system/numbers/location/{locationId}",
    executionParameters: [{"name":"locationId","in":"path"},{"name":"pageSize","in":"query"},{"name":"page","in":"query"},{"name":"searchFilter","in":"query"},{"name":"skipNumberPool","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["phonenumbers.read"]}]
  }],
  ["bulkUpdate", {
    name: "bulkUpdate",
    description: `API to bulk update products (price, availability, collections, delete)`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"type":{"type":"string","enum":["bulk-update-price","bulk-update-availability","bulk-update-product-collection","bulk-delete-products","bulk-update-currency"],"description":"Type of bulk update operation"},"productIds":{"description":"Array of product IDs","type":"array","items":{"type":"string"}},"filters":{"description":"Filters to apply when selectAll is true","allOf":[{"type":"object","properties":{"collectionIds":{"description":"Filter by collection IDs","example":["5f8d0d55b54764421b7156c1","5f8d0d55b54764421b7156c2"],"type":"array","items":{"type":"string"}},"productType":{"type":"string","description":"Filter by product type","example":"one-time"},"availableInStore":{"type":"boolean","description":"Filter by availability status","example":true},"search":{"type":"string","description":"Filter by search term","example":"blue t-shirt"}}}]},"price":{"description":"Price update configuration","allOf":[{"type":"object","properties":{"type":{"type":"string","enum":["INCREASE_BY_AMOUNT","REDUCE_BY_AMOUNT","SET_NEW_PRICE","INCREASE_BY_PERCENTAGE","REDUCE_BY_PERCENTAGE"],"description":"Type of price update","example":"INCREASE_BY_AMOUNT"},"value":{"type":"number","example":100,"description":"Value to update (amount or percentage based on type)"},"roundToWhole":{"type":"boolean","description":"Round to nearest whole number","example":true}},"required":["type","value"]}]},"compareAtPrice":{"description":"Compare at price update configuration","allOf":[{"type":"object","properties":{"type":{"type":"string","enum":["INCREASE_BY_AMOUNT","REDUCE_BY_AMOUNT","SET_NEW_PRICE","INCREASE_BY_PERCENTAGE","REDUCE_BY_PERCENTAGE"],"description":"Type of price update","example":"INCREASE_BY_AMOUNT"},"value":{"type":"number","example":100,"description":"Value to update (amount or percentage based on type)"},"roundToWhole":{"type":"boolean","description":"Round to nearest whole number","example":true}},"required":["type","value"]}]},"availability":{"type":"boolean","description":"New availability status"},"collectionIds":{"description":"Array of collection IDs","type":"array","items":{"type":"string"}},"currency":{"type":"string","description":"Currency code"}},"required":["altId","altType","type","productIds"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products/bulk-update",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]}]
  }],
  ["bulkEdit", {
    name: "bulkEdit",
    description: `API to bulk edit products and their associated prices (max 30 entities)`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"products":{"description":"Array of products to update. Note: The total count includes all prices within each product.","type":"array","items":{"type":"object","properties":{"_id":{"type":"string","description":"Product ID"},"name":{"type":"string","description":"Product name"},"description":{"type":"string","description":"Product description"},"image":{"type":"string","description":"Product image"},"availableInStore":{"type":"boolean","description":"Product availability in store"},"prices":{"description":"Array of price variants for the product","type":"array","items":{"type":"object","properties":{"_id":{"type":"string","description":"Price ID"},"name":{"type":"string","description":"Price name"},"amount":{"type":"number","description":"Price amount"},"currency":{"type":"string","description":"Price currency"},"compareAtPrice":{"type":"number","description":"Compare at price"},"availableQuantity":{"type":"number","description":"Available quantity"},"trackInventory":{"type":"boolean","description":"Track inventory"},"allowOutOfStockPurchases":{"type":"boolean","description":"Allow out of stock purchases"},"sku":{"type":"string","description":"SKU"},"trialPeriod":{"type":"number","description":"Trial period in days"},"totalCycles":{"type":"number","description":"Total billing cycles"},"setupFee":{"type":"number","description":"Setup fee"},"shippingOptions":{"description":"Shipping options","allOf":[{"type":"object","properties":{"weight":{"description":"Weight options of the product","allOf":[{"type":"object","properties":{"value":{"type":"number","description":"Actual weight of the product","example":10},"unit":{"type":"string","description":"Weight unit of the product","example":"kg","enum":["kg","lb","g","oz"]}},"required":["value","unit"]}]},"dimensions":{"description":"Dimensions of the product","allOf":[{"type":"object","properties":{"height":{"type":"number","description":"Height of the price","example":10},"width":{"type":"number","description":"Width of the price","example":10},"length":{"type":"number","description":"Length of the price","example":10},"unit":{"type":"string","description":"Unit of the price dimensions","example":"cm","enum":["cm","in","m"]}},"required":["height","width","length","unit"]}]}}}]},"recurring":{"description":"Recurring details","allOf":[{"type":"object","properties":{"interval":{"type":"string","description":"The interval at which the recurring event occurs.","example":"day","enum":["day","month","week","year"]},"intervalCount":{"type":"number","description":"The number of intervals between each occurrence of the event.","example":1}},"required":["interval","intervalCount"]}]}},"required":["_id"]}},"collectionIds":{"description":"Collection IDs","type":"array","items":{"type":"string"}},"isLabelEnabled":{"type":"boolean","description":"Enable product label"},"isTaxesEnabled":{"type":"boolean","description":"Enable taxes"},"seo":{"description":"SEO metadata for the product","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"SEO title","example":"Best Product - Buy Now"},"description":{"type":"string","description":"SEO description","example":"This is the best product you can buy online with amazing features and great value"}}}]},"slug":{"type":"string","description":"Product URL slug"},"automaticTaxCategoryId":{"type":"string","description":"Automatic tax category ID"},"taxInclusive":{"type":"boolean","description":"Tax inclusive pricing"},"taxes":{"description":"Product taxes","type":"array","items":{"type":"object"}},"medias":{"description":"Product media","type":"array","items":{"type":"object"}},"label":{"type":"object","description":"Product label"}},"required":["_id"]}}},"required":["altId","altType","products"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products/bulk-update/edit",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["list-prices-for-product", {
    name: "list-prices-for-product",
    description: `The "List Prices for a Product" API allows retrieving a paginated list of prices associated with a specific product. Customize your results by filtering prices or paginate through the list using the provided query parameters.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID of the product that needs to be used"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"locationId":{"type":"string","description":"The unique identifier for the location."},"ids":{"type":"string","description":"To filter the response only with the given price ids, Please provide with comma separated"}},"required":["Version","productId","locationId"]},
    method: "get",
    pathTemplate: "/products/{productId}/price",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"locationId","in":"query"},{"name":"ids","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/prices.readonly"]},{"Agency-Access":["products/prices.readonly"]}]
  }],
  ["create-price-for-product", {
    name: "create-price-for-product",
    description: `The "Create Price for a Product" API allows adding a new price associated with a specific product to the system. Use this endpoint to create a price with the specified details for a particular product. Ensure that the required information is provided in the request payload.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID of the product that needs to be used"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name of the price."},"type":{"type":"string","description":"The type of the price.","enum":["one_time","recurring"]},"currency":{"type":"string","description":"The currency of the price."},"amount":{"type":"number","description":"The amount of the price. ( min: 0 )"},"recurring":{"description":"The recurring details of the price (if type is recurring).","allOf":[{"type":"object","properties":{"interval":{"type":"string","description":"The interval at which the recurring event occurs.","example":"day","enum":["day","month","week","year"]},"intervalCount":{"type":"number","description":"The number of intervals between each occurrence of the event.","example":1}},"required":["interval","intervalCount"]}]},"description":{"type":"string","description":"A brief description of the price."},"membershipOffers":{"description":"An array of membership offers associated with the price.","type":"array","items":{"type":"object","properties":{"label":{"type":"string","description":"Membership offer label"},"value":{"type":"string","description":"Membership offer label"},"_id":{"type":"string","description":"The unique identifier for the membership offer."}},"required":["label","value","_id"]}},"trialPeriod":{"type":"number","description":"The trial period duration in days (if applicable)."},"totalCycles":{"type":"number","description":"The total number of billing cycles for the price. ( min: 1 )"},"setupFee":{"type":"number","description":"The setup fee for the price."},"variantOptionIds":{"description":"An array of variant option IDs associated with the price.","type":"array","items":{"type":"string"}},"compareAtPrice":{"type":"number","description":"The compare at price for the price."},"locationId":{"type":"string","description":"The unique identifier of the location associated with the price."},"userId":{"type":"string","description":"The unique identifier of the user who created the price."},"meta":{"description":"Additional metadata associated with the price.","allOf":[{"type":"object","properties":{"source":{"type":"string","description":"The source of the price.","enum":["stripe","woocommerce","shopify"],"example":"stripe"},"sourceId":{"type":"string","description":"The id of the source of the price from where it is imported","example":"123"},"stripePriceId":{"type":"string","description":"The Stripe price ID associated with the price.","example":"price_123"},"internalSource":{"type":"string","description":"The internal source of the price.","enum":["agency_plan","funnel","membership","communities","gokollab","calendar"],"example":"agency_plan"}},"required":["source","stripePriceId","internalSource"]}]},"trackInventory":{"type":"boolean","description":"Need to track inventory stock quantity"},"availableQuantity":{"type":"number","description":"Available inventory stock quantity"},"allowOutOfStockPurchases":{"type":"boolean","description":"Continue selling when out of stock"},"sku":{"type":"string","description":"The unique identifier of the SKU associated with the price"},"shippingOptions":{"description":"Shipping options of the Price","allOf":[{"type":"object","properties":{"weight":{"description":"Weight options of the product","allOf":[{"type":"object","properties":{"value":{"type":"number","description":"Actual weight of the product","example":10},"unit":{"type":"string","description":"Weight unit of the product","example":"kg","enum":["kg","lb","g","oz"]}},"required":["value","unit"]}]},"dimensions":{"description":"Dimensions of the product","allOf":[{"type":"object","properties":{"height":{"type":"number","description":"Height of the price","example":10},"width":{"type":"number","description":"Width of the price","example":10},"length":{"type":"number","description":"Length of the price","example":10},"unit":{"type":"string","description":"Unit of the price dimensions","example":"cm","enum":["cm","in","m"]}},"required":["height","width","length","unit"]}]}}}]},"isDigitalProduct":{"type":"boolean","description":"Is the product a digital product"},"digitalDelivery":{"description":"Digital delivery options","type":"array","items":{"type":"string"}}},"required":["name","type","currency","amount","locationId"],"description":"The JSON request body."}},"required":["Version","productId","requestBody"]},
    method: "post",
    pathTemplate: "/products/{productId}/price",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products/prices.write"]},{"Agency-Access":["products/prices.write"]}]
  }],
  ["get-list-inventory", {
    name: "get-list-inventory",
    description: `The "List Inventory API allows the user to retrieve a paginated list of inventory items. Use this endpoint to fetch details for multiple items in the inventory based on the provided query parameters.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"search":{"type":"string","description":"Search string for Variant Search"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/products/inventory",
    executionParameters: [{"name":"Version","in":"header"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"search","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/prices.readonly"]},{"Agency-Access":["products/prices.readonly"]}]
  }],
  ["update-inventory", {
    name: "update-inventory",
    description: `The Update Inventory API allows the user to bulk update the inventory for multiple items. Use this endpoint to update the available quantity and out-of-stock purchase settings for multiple items in the inventory.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"items":{"description":"Array of items to update in the inventory.","type":"array","items":{"type":"object","properties":{"priceId":{"type":"string","description":"The unique identifier for the price, in MongoDB ID format."},"availableQuantity":{"type":"number","description":"The available quantity of the item."},"allowOutOfStockPurchases":{"type":"boolean","description":"Whether to continue selling the item when out of stock."}},"required":["priceId"]}}},"required":["altId","altType","items"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products/inventory",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products/prices.write"]},{"Agency-Access":["products/prices.write"]}]
  }],
  ["get-price-by-id-for-product", {
    name: "get-price-by-id-for-product",
    description: `The "Get Price by ID for a Product" API allows retrieving information for a specific price associated with a particular product using its unique identifier. Use this endpoint to fetch details for a single price based on the provided price ID and product ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID of the product that needs to be used"},"priceId":{"type":"string","description":"ID of the price that needs to be returned"},"locationId":{"type":"string","description":"location Id"}},"required":["Version","productId","priceId","locationId"]},
    method: "get",
    pathTemplate: "/products/{productId}/price/{priceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"priceId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/prices.readonly"]},{"Agency-Access":["products/prices.readonly"]}]
  }],
  ["update-price-by-id-for-product", {
    name: "update-price-by-id-for-product",
    description: `The "Update Price by ID for a Product" API allows modifying information for a specific price associated with a particular product using its unique identifier. Use this endpoint to update details for a single price based on the provided price ID and product ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID of the product that needs to be used"},"priceId":{"type":"string","description":"ID of the price that needs to be returned"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name of the price."},"type":{"type":"string","description":"The type of the price.","enum":["one_time","recurring"]},"currency":{"type":"string","description":"The currency of the price."},"amount":{"type":"number","description":"The amount of the price. ( min: 0 )"},"recurring":{"description":"The recurring details of the price (if type is recurring).","allOf":[{"type":"object","properties":{"interval":{"type":"string","description":"The interval at which the recurring event occurs.","example":"day","enum":["day","month","week","year"]},"intervalCount":{"type":"number","description":"The number of intervals between each occurrence of the event.","example":1}},"required":["interval","intervalCount"]}]},"description":{"type":"string","description":"A brief description of the price."},"membershipOffers":{"description":"An array of membership offers associated with the price.","type":"array","items":{"type":"object","properties":{"label":{"type":"string","description":"Membership offer label"},"value":{"type":"string","description":"Membership offer label"},"_id":{"type":"string","description":"The unique identifier for the membership offer."}},"required":["label","value","_id"]}},"trialPeriod":{"type":"number","description":"The trial period duration in days (if applicable)."},"totalCycles":{"type":"number","description":"The total number of billing cycles for the price. ( min: 1 )"},"setupFee":{"type":"number","description":"The setup fee for the price."},"variantOptionIds":{"description":"An array of variant option IDs associated with the price.","type":"array","items":{"type":"string"}},"compareAtPrice":{"type":"number","description":"The compare at price for the price."},"locationId":{"type":"string","description":"The unique identifier of the location associated with the price."},"userId":{"type":"string","description":"The unique identifier of the user who created the price."},"meta":{"description":"Additional metadata associated with the price.","allOf":[{"type":"object","properties":{"source":{"type":"string","description":"The source of the price.","enum":["stripe","woocommerce","shopify"],"example":"stripe"},"sourceId":{"type":"string","description":"The id of the source of the price from where it is imported","example":"123"},"stripePriceId":{"type":"string","description":"The Stripe price ID associated with the price.","example":"price_123"},"internalSource":{"type":"string","description":"The internal source of the price.","enum":["agency_plan","funnel","membership","communities","gokollab","calendar"],"example":"agency_plan"}},"required":["source","stripePriceId","internalSource"]}]},"trackInventory":{"type":"boolean","description":"Need to track inventory stock quantity"},"availableQuantity":{"type":"number","description":"Available inventory stock quantity"},"allowOutOfStockPurchases":{"type":"boolean","description":"Continue selling when out of stock"},"sku":{"type":"string","description":"The unique identifier of the SKU associated with the price"},"shippingOptions":{"description":"Shipping options of the Price","allOf":[{"type":"object","properties":{"weight":{"description":"Weight options of the product","allOf":[{"type":"object","properties":{"value":{"type":"number","description":"Actual weight of the product","example":10},"unit":{"type":"string","description":"Weight unit of the product","example":"kg","enum":["kg","lb","g","oz"]}},"required":["value","unit"]}]},"dimensions":{"description":"Dimensions of the product","allOf":[{"type":"object","properties":{"height":{"type":"number","description":"Height of the price","example":10},"width":{"type":"number","description":"Width of the price","example":10},"length":{"type":"number","description":"Length of the price","example":10},"unit":{"type":"string","description":"Unit of the price dimensions","example":"cm","enum":["cm","in","m"]}},"required":["height","width","length","unit"]}]}}}]},"isDigitalProduct":{"type":"boolean","description":"Is the product a digital product"},"digitalDelivery":{"description":"Digital delivery options","type":"array","items":{"type":"string"}}},"required":["name","type","currency","amount","locationId"],"description":"The JSON request body."}},"required":["Version","productId","priceId","requestBody"]},
    method: "put",
    pathTemplate: "/products/{productId}/price/{priceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"priceId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products/prices.write"]},{"Agency-Access":["products/prices.write"]}]
  }],
  ["delete-price-by-id-for-product", {
    name: "delete-price-by-id-for-product",
    description: `The "Delete Price by ID for a Product" API allows deleting a specific price associated with a particular product using its unique identifier. Use this endpoint to remove a price from the system.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID of the product that needs to be used"},"priceId":{"type":"string","description":"ID of the price that needs to be returned"},"locationId":{"type":"string","description":"location Id"}},"required":["Version","productId","priceId","locationId"]},
    method: "delete",
    pathTemplate: "/products/{productId}/price/{priceId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"priceId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/prices.write"]},{"Agency-Access":["products/prices.write"]}]
  }],
  ["get-product-store-stats", {
    name: "get-product-store-stats",
    description: `API to fetch the total number of products, included in the store, and excluded from the store and other stats`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"storeId":{"type":"string","description":"Products related to the store"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"search":{"type":"string","description":"The name of the product for searching."},"collectionIds":{"type":"string","description":"Filter by product collection Ids. Supports comma separated values"}},"required":["Version","storeId","altId","altType"]},
    method: "get",
    pathTemplate: "/products/store/{storeId}/stats",
    executionParameters: [{"name":"Version","in":"header"},{"name":"storeId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"search","in":"query"},{"name":"collectionIds","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.readonly"]}]
  }],
  ["update-store-status", {
    name: "update-store-status",
    description: `API to update the status of products in a particular store`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"storeId":{"type":"string","description":"Products related to the store"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"action":{"type":"string","description":"Action to include or exclude the product from the store","enum":["include","exclude"]},"productIds":{"description":"Array of product IDs","type":"array","items":{"type":"string"}}},"required":["altId","altType","action","productIds"],"description":"The JSON request body."}},"required":["Version","storeId","requestBody"]},
    method: "post",
    pathTemplate: "/products/store/{storeId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"storeId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]}]
  }],
  ["update-display-priority", {
    name: "update-display-priority",
    description: `API to set the display priority of products in a store`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"storeId":{"type":"string","description":"Products related to the store"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"products":{"description":"Array of products with their display priorities","items":{"type":"array","items":{"type":"object"}},"type":"array"}},"required":["altId","altType","products"],"description":"The JSON request body."}},"required":["Version","storeId","requestBody"]},
    method: "post",
    pathTemplate: "/products/store/{storeId}/priority",
    executionParameters: [{"name":"Version","in":"header"},{"name":"storeId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-product-collection", {
    name: "get-product-collection",
    description: `Internal API to fetch the Product Collections`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"limit":{"default":10,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"altId":{"type":"string","description":"Location Id"},"altType":{"enum":["location"],"type":"string","description":"The type of alt. For now it is only LOCATION"},"collectionIds":{"type":"string","description":"Ids of the collections separated by comma(,) for search purposes"},"name":{"type":"string","description":"Query to search collection based on names"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/products/collections",
    executionParameters: [{"name":"Version","in":"header"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"collectionIds","in":"query"},{"name":"name","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/collection.readonly"]}]
  }],
  ["create-product-collection", {
    name: "create-product-collection",
    description: `Create a new Product Collection for a specific location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"The type of alt. For now it is only LOCATION","enum":["location"]},"collectionId":{"type":"string","description":"Unique Identifier of the Product Collection, Mongo Id"},"name":{"type":"string","description":"Name of the Product Collection"},"slug":{"type":"string","description":"Slug of the Product Collection which helps in navigation"},"image":{"type":"string","description":"The URL of the image that is going to be displayed as the collection Thumbnail"},"seo":{"description":"The metadata information which will be displayed in SEO previews","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"The title which will be displayed as an SEO format","example":"Best Sellers"},"description":{"type":"string","description":"The description which would be displayed in preview purposes","example":"Collections where all the best products are available"}}}]}},"required":["altId","altType","name","slug"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products/collections",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products/collection.write"]}]
  }],
  ["get-product-collection-id", {
    name: "get-product-collection-id",
    description: `Get Details about individual product collection`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"collectionId":{"type":"string","description":"Collection Id"},"altId":{"type":"string","description":"Location Id"}},"required":["Version","collectionId","altId"]},
    method: "get",
    pathTemplate: "/products/collections/{collectionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"collectionId","in":"path"},{"name":"altId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/collection.readonly"]}]
  }],
  ["update-product-collection", {
    name: "update-product-collection",
    description: `Update a specific product collection with Id :collectionId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"collectionId":{"type":"string","description":"MongoId of the collection"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id"},"altType":{"type":"string","description":"The type of alt. For now it is only LOCATION","enum":["location"]},"name":{"type":"string","description":"Name of the Product Collection"},"slug":{"type":"string","description":"Slug of the Product Collection which helps in navigation"},"image":{"type":"string","description":"The URL of the image that is going to be displayed as the collection Thumbnail"},"seo":{"description":"The metadata information which will be displayed in SEO previews","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"The title which will be displayed as an SEO format","example":"Best Sellers"},"description":{"type":"string","description":"The description which would be displayed in preview purposes","example":"Collections where all the best products are available"}}}]}},"required":["altId","altType"],"description":"The JSON request body."}},"required":["Version","collectionId","requestBody"]},
    method: "put",
    pathTemplate: "/products/collections/{collectionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"collectionId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products/collection.write"]}]
  }],
  ["delete-product-collection", {
    name: "delete-product-collection",
    description: `Delete specific product collection with Id :collectionId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"collectionId":{"type":"string","description":"MongoId of the collection"},"altId":{"type":"string","description":"Location Id"},"altType":{"enum":["location"],"type":"string","description":"The type of alt. For now it is only LOCATION"}},"required":["Version","collectionId","altId","altType"]},
    method: "delete",
    pathTemplate: "/products/collections/{collectionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"collectionId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products/collection.write"]}]
  }],
  ["get-product-reviews", {
    name: "get-product-reviews",
    description: `API to fetch the Product Reviews`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"sortField":{"enum":["createdAt","rating"],"type":"string","description":"The field upon which the sort should be applied"},"sortOrder":{"enum":["asc","desc"],"type":"string","description":"The order of sort which should be applied for the sortField"},"rating":{"type":"number","description":"Key to filter the ratings "},"startDate":{"type":"string","description":"The start date for filtering reviews"},"endDate":{"type":"string","description":"The end date for filtering reviews"},"productId":{"type":"string","description":"Comma-separated list of product IDs"},"storeId":{"type":"string","description":"Comma-separated list of store IDs"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/products/reviews",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"sortField","in":"query"},{"name":"sortOrder","in":"query"},{"name":"rating","in":"query"},{"name":"startDate","in":"query"},{"name":"endDate","in":"query"},{"name":"productId","in":"query"},{"name":"storeId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.readonly"]}]
  }],
  ["get-reviews-count", {
    name: "get-reviews-count",
    description: `API to fetch the Review Count as per status`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"rating":{"type":"number","description":"Key to filter the ratings "},"startDate":{"type":"string","description":"The start date for filtering reviews"},"endDate":{"type":"string","description":"The end date for filtering reviews"},"productId":{"type":"string","description":"Comma-separated list of product IDs"},"storeId":{"type":"string","description":"Comma-separated list of store IDs"}},"required":["Version","altId","altType"]},
    method: "get",
    pathTemplate: "/products/reviews/count",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"rating","in":"query"},{"name":"startDate","in":"query"},{"name":"endDate","in":"query"},{"name":"productId","in":"query"},{"name":"storeId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.readonly"]}]
  }],
  ["update-product-review", {
    name: "update-product-review",
    description: `Update status, reply, etc of a particular review`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"reviewId":{"type":"string","description":"Review Id"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"productId":{"type":"string","description":"Product Id"},"status":{"type":"string","description":"Status of the review"},"reply":{"description":"Reply of the review","type":"array","items":{"type":"object","properties":{"headline":{"type":"string","description":"Headline of the Review","minLength":0,"maxLength":200},"comment":{"type":"string","description":"Detailed Review of the product","minLength":0,"maxLength":5000},"user":{"description":"User who is giving the review/reply","allOf":[{"type":"object","properties":{"name":{"type":"string","description":"Name of the customer","example":"John Doe","minLength":1,"maxLength":100},"email":{"type":"string","description":"Email of the customer","example":"example@example.com"},"phone":{"type":"string","description":"Phone no of the customer","example":"+1-555-555-5555"},"isCustomer":{"type":"boolean","description":"Is the person an admin or customer","example":true}},"required":["name","email"]}]}},"required":["headline","comment","user"]}},"rating":{"type":"number","description":"Rating of the product"},"headline":{"type":"string","description":"Headline of the Review"},"detail":{"type":"string","description":"Detailed Review of the product"}},"required":["altId","altType","productId","status"],"description":"The JSON request body."}},"required":["Version","reviewId","requestBody"]},
    method: "put",
    pathTemplate: "/products/reviews/{reviewId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"reviewId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]}]
  }],
  ["delete-product-review", {
    name: "delete-product-review",
    description: `Delete specific product review`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"reviewId":{"type":"string","description":"Review Id"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"productId":{"type":"string","description":"Product Id of the product"}},"required":["Version","reviewId","altId","altType","productId"]},
    method: "delete",
    pathTemplate: "/products/reviews/{reviewId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"reviewId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"productId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.write"]}]
  }],
  ["bulk-update-product-review", {
    name: "bulk-update-product-review",
    description: `Update one or multiple product reviews: status, reply, etc.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"reviews":{"description":"Array of Product Reviews","type":"array","items":{"type":"object","properties":{"reviewId":{"type":"string","description":"Review Id"},"productId":{"type":"string","description":"Product Id"},"storeId":{"type":"string","description":"Store Id"}},"required":["reviewId","productId","storeId"]}},"status":{"type":"object","description":"Status of the review"}},"required":["altId","altType","reviews","status"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products/reviews/bulk-update",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]}]
  }],
  ["get-product-by-id", {
    name: "get-product-by-id",
    description: `The "Get Product by ID" API allows to retrieve information for a specific product using its unique identifier. Use this endpoint to fetch details for a single product based on the provided product ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID or the slug of the product that needs to be returned"},"locationId":{"type":"string","description":"location Id"},"sendWishlistStatus":{"type":"boolean","description":"Parameter which will decide whether to show the wishlisting status of products"}},"required":["Version","productId","locationId"]},
    method: "get",
    pathTemplate: "/products/{productId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"locationId","in":"query"},{"name":"sendWishlistStatus","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.readonly"]},{"Agency-Access":["products.readonly"]}]
  }],
  ["update-product-by-id", {
    name: "update-product-by-id",
    description: `The "Update Product by ID" API allows modifying information for a specific product using its unique identifier. Use this endpoint to update details for a single product based on the provided product ID.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID or the slug of the product that needs to be returned"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name of the product."},"locationId":{"type":"string","description":"The unique identifier for the location."},"description":{"type":"string","description":"A brief description of the product."},"productType":{"type":"string","enum":["DIGITAL","PHYSICAL","SERVICE","PHYSICAL/DIGITAL"]},"image":{"type":"string","description":"The URL for the product image."},"statementDescriptor":{"type":"string","description":"The statement descriptor for the product."},"availableInStore":{"type":"boolean","description":"Indicates whether the product is available in-store."},"medias":{"description":"An array of medias for the product.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"The unique identifier for the media."},"title":{"type":"string","description":"The title of the media file."},"url":{"type":"string","description":"The URL where the media file is stored."},"type":{"type":"string","description":"The type of the media file (e.g., image, video will be supporting soon).","enum":["image","video"]},"isFeatured":{"type":"boolean","description":"Indicates whether the media is featured."},"priceIds":{"description":"Mongo ObjectIds of the prices for which the media is assigned","type":"array","items":{"type":"array","items":{"type":"object"}}}},"required":["id","url","type"]}},"variants":{"description":"An array of variants for the product.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"A unique identifier for the variant."},"name":{"type":"string","description":"The name of the variant."},"options":{"description":"An array of options for the variant.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"The unique identifier for the option."},"name":{"type":"string","description":"The name of the option."}},"required":["id","name"]}}},"required":["id","name","options"]}},"collectionIds":{"description":"An array of category Ids for the product","type":"array","items":{"type":"string"}},"isTaxesEnabled":{"type":"boolean","description":"Are there any taxes attached to the product. If this is true, taxes array cannot be empty.","default":false},"taxes":{"description":"List of ids of Taxes attached to the Product. If taxes are passed, isTaxesEnabled should be true.","type":"array","items":{"type":"string"}},"automaticTaxCategoryId":{"type":"string","description":"Tax category ID for Automatic taxes calculation."},"isLabelEnabled":{"type":"boolean","description":"Is the product label enabled. If this is true, label object cannot be empty.","default":false},"label":{"description":"Details for Product Label","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"The content for the product label.","example":"Featured"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format","example":"2024-06-26T05:43:35.000Z"},"endDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format","example":"2024-06-30T05:43:39.000Z"}},"required":["title"]}]},"slug":{"type":"string","description":"The slug using which the product navigation will be handled"},"seo":{"description":"SEO data for the product that will be displayed in the preview","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"SEO title","example":"Best Product - Buy Now"},"description":{"type":"string","description":"SEO description","example":"This is the best product you can buy online with amazing features and great value"}}}]},"taxInclusive":{"type":"boolean","description":"Whether the taxes should be included in the purchase price","default":false},"prices":{"description":"The prices of the product","type":"array","items":{"type":"string"}}},"required":["name","locationId","productType"],"description":"The JSON request body."}},"required":["Version","productId","requestBody"]},
    method: "put",
    pathTemplate: "/products/{productId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]},{"Agency-Access":["products.write"]}]
  }],
  ["delete-product-by-id", {
    name: "delete-product-by-id",
    description: `The "Delete Product by ID" API allows deleting a specific product using its unique identifier. Use this endpoint to remove a product from the system.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"productId":{"type":"string","description":"ID or the slug of the product that needs to be returned"},"locationId":{"type":"string","description":"location Id"},"sendWishlistStatus":{"type":"boolean","description":"Parameter which will decide whether to show the wishlisting status of products"}},"required":["Version","productId","locationId"]},
    method: "delete",
    pathTemplate: "/products/{productId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"productId","in":"path"},{"name":"locationId","in":"query"},{"name":"sendWishlistStatus","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.write"]},{"Agency-Access":["products.write"]}]
  }],
  ["list-documents-contracts", {
    name: "list-documents-contracts",
    description: `List documents for a location`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"locationId":{"type":"string","description":"Location Id"},"status":{"enum":["draft","sent","viewed","completed","accepted"],"type":"string","description":"Document status, pass as comma separated values"},"paymentStatus":{"enum":["waiting_for_payment","paid","no_payment"],"type":"string","description":"Payment status, pass as comma separated values"},"limit":{"type":"number","description":"Limit to fetch number of records"},"skip":{"type":"number","description":"Skip number of records"},"query":{"type":"string","description":"Search string"},"dateFrom":{"type":"string","description":"Date start from (ISO 8601), dateFrom & DateTo must be provided together"},"dateTo":{"type":"string","description":"Date to (ISO 8601), dateFrom & DateTo must be provided together"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["Authorization","locationId","Version"]},
    method: "get",
    pathTemplate: "/proposals/document",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"locationId","in":"query"},{"name":"status","in":"query"},{"name":"paymentStatus","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"},{"name":"query","in":"query"},{"name":"dateFrom","in":"query"},{"name":"dateTo","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["send-documents-contracts", {
    name: "send-documents-contracts",
    description: `Send document to a client`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location Id"},"documentId":{"type":"string","description":"Document Id"},"documentName":{"type":"string","description":"Document Name"},"medium":{"type":"string","enum":["link","email"],"description":"Medium to be used for sending the document"},"ccRecipients":{"description":"CC Recipient","type":"array","items":{"type":"object","properties":{"email":{"type":"string","description":"Email"},"id":{"type":"string","description":"Contact ID"},"imageUrl":{"type":"string","description":"Contact Image URL"},"contactName":{"type":"string","description":"Contact Name"},"firstName":{"type":"string","description":"First Name"},"lastName":{"type":"string","description":"Last Name"}},"required":["email","id","imageUrl","contactName","firstName","lastName"]}},"notificationSettings":{"allOf":[{"type":"object","properties":{"receive":{"type":"object","properties":{"templateId":{"type":"string"},"subject":{"type":"string"}},"required":["templateId","subject"]},"sender":{"type":"object","properties":{"fromEmail":{"type":"string"},"fromName":{"type":"string"}},"required":["fromEmail","fromName"]}},"required":["receive","sender"]}]},"sentBy":{"type":"string","description":"Sent ByUser Id"}},"required":["locationId","documentId","sentBy"],"description":"The JSON request body."}},"required":["Authorization","Version","requestBody"]},
    method: "post",
    pathTemplate: "/proposals/document/send",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["list-documents-contracts-templates", {
    name: "list-documents-contracts-templates",
    description: `List document contract templates for a location`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"locationId":{"type":"string","description":"Location Id"},"dateFrom":{"type":"string","description":"Date start from (ISO 8601)"},"dateTo":{"type":"string","description":"Date to (ISO 8601)"},"type":{"type":"string","description":"Comma-separated template types. Valid values: proposal, estimate, contentLibrary"},"name":{"type":"string","description":"Template Name"},"isPublicDocument":{"type":"boolean","description":"If the docForm is a DocForm"},"userId":{"type":"string","description":"User Id, required when isPublicDocument is true"},"limit":{"type":"string","description":"Limit"},"skip":{"type":"string","description":"Skip"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["Authorization","locationId","Version"]},
    method: "get",
    pathTemplate: "/proposals/templates",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"locationId","in":"query"},{"name":"dateFrom","in":"query"},{"name":"dateTo","in":"query"},{"name":"type","in":"query"},{"name":"name","in":"query"},{"name":"isPublicDocument","in":"query"},{"name":"userId","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["send-documents-contracts-template", {
    name: "send-documents-contracts-template",
    description: `Send template to a client`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"templateId":{"type":"string","description":"Template Id"},"userId":{"type":"string","description":"User Id"},"sendDocument":{"type":"boolean","description":"Send Document"},"locationId":{"type":"string","description":"Location Id"},"contactId":{"type":"string","description":"Contact Id"},"opportunityId":{"type":"string","description":"Opportunity Id"}},"required":["templateId","userId","locationId","contactId"],"description":"The JSON request body."}},"required":["Authorization","Version","requestBody"]},
    method: "post",
    pathTemplate: "/proposals/templates/send",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]},{"Agency-Access":[]}]
  }],
  ["locations-deprecated", {
    name: "locations-deprecated",
    description: `Get locations by stripeCustomerId or stripeSubscriptionId with companyId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"customerId":{"type":"string","description":"Stripe customer ID to find locations for"},"subscriptionId":{"type":"string","description":"Stripe subscription ID to find locations for"},"companyId":{"type":"string","description":"Company ID to filter locations"}},"required":["Version","companyId"]},
    method: "get",
    pathTemplate: "/saas-api/public-api/locations",
    executionParameters: [{"name":"Version","in":"header"},{"name":"customerId","in":"query"},{"name":"subscriptionId","in":"query"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["update-saas-subscription-deprecated", {
    name: "update-saas-subscription-deprecated",
    description: `Update SaaS subscription for given locationId and customerId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID to update subscription for"},"requestBody":{"type":"object","properties":{"subscriptionId":{"type":"string","description":"Subscription ID"},"customerId":{"type":"string","description":"Customer ID"},"companyId":{"type":"string","description":"Company ID"}},"required":["subscriptionId","customerId","companyId"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "put",
    pathTemplate: "/saas-api/public-api/update-saas-subscription/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["bulk-disable-saas-deprecated", {
    name: "bulk-disable-saas-deprecated",
    description: `Disable SaaS for locations for given locationIds`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID to disable SaaS for"},"requestBody":{"type":"object","properties":{"locationIds":{"description":"Location IDs","type":"array","items":{"type":"string"}}},"required":["locationIds"],"description":"The JSON request body."}},"required":["Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas-api/public-api/bulk-disable-saas/{companyId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["enable-saas-location-deprecated", {
    name: "enable-saas-location-deprecated",
    description: `<div>
                  <p>Enable SaaS for Sub-Account (Formerly Location) based on the data provided</p>
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                          This feature is only available on Agency Pro ($497) plan.
                        </strong>
                      </span>
                  </div>
                </div>
    `,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID to enable SaaS for"},"requestBody":{"type":"object","properties":{"stripeAccountId":{"type":"string","description":"Stripe account id(Required only for SaaS V1)"},"name":{"type":"string","description":"Name of the stripe customer(Required only for SaaS V1)"},"email":{"type":"string","description":"Email of the stripe customer(Required only for SaaS V1)"},"stripeCustomerId":{"type":"string","description":"Stripe customer id if exists(Required only for SaaS V1)"},"companyId":{"type":"string"},"isSaaSV2":{"type":"boolean","description":"Denotes if it is a saas v2 or v1 sub-account"},"contactId":{"type":"string","description":"Agency subaccount used for payment provider integration(Required Only for SaaS V2)"},"providerLocationId":{"type":"string","description":"Agency Subaccount ID"},"description":{"type":"string","description":"Description"},"saasPlanId":{"type":"string","description":"Required only while pre-configuring saas subscription"},"priceId":{"type":"string","description":"Required only while pre-configuring saas subscription"}},"required":["companyId","isSaaSV2"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/saas-api/public-api/enable-saas/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["pause-location-deprecated", {
    name: "pause-location-deprecated",
    description: `Pause Sub account for given locationId`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID to pause/unpause"},"requestBody":{"type":"object","properties":{"paused":{"type":"boolean","description":"Paused"},"companyId":{"type":"string","description":"Company ID"}},"required":["paused","companyId"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/saas-api/public-api/pause/{locationId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["update-rebilling-deprecated", {
    name: "update-rebilling-deprecated",
    description: `Bulk update rebilling for given locationIds`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID to update rebilling for"},"requestBody":{"type":"object","properties":{"product":{"type":"string","description":"The product to update rebilling for","enum":["contentAI","workflow_premium_actions","workflow_ai","conversationAI","EmailNotification","whatsApp","reviewsAI","VERIFIED_CALLER_ID","WALLET_SALES_TAX","NOTIFICATION_SMS","EmailSmtp","EmailVerification","autoCompleteAddress","funnelAI","domainPurchase","Phone","Email"]},"locationIds":{"description":"Array of location IDs to update rebilling for","type":"array","items":{"type":"string"}},"config":{"type":"object","description":"Configuration for rebilling settings","properties":{"optIn":{"type":"boolean","description":"Enable the product for the locations"},"enabled":{"type":"boolean","description":"Enable the rebilling for the locations"},"markup":{"type":"number","description":"Additional value to be added in terms of percentage. For example, if the product price is $100 and the markup is 5, the amount charged to will be $105."}}}},"required":["product","locationIds","config"],"description":"The JSON request body."}},"required":["Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas-api/public-api/update-rebilling/{companyId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-agency-plans-deprecated", {
    name: "get-agency-plans-deprecated",
    description: `Fetch all agency subscription plans for a given company ID`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID to get agency plans for"}},"required":["Authorization","Version","companyId"]},
    method: "get",
    pathTemplate: "/saas-api/public-api/agency-plans/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-location-subscription-deprecated", {
    name: "get-location-subscription-deprecated",
    description: `Fetch subscription details for a specific location from location metadata`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID to get subscription details for"},"companyId":{"type":"string","description":"Company ID to filter subscription details"}},"required":["Authorization","Version","locationId","companyId"]},
    method: "get",
    pathTemplate: "/saas-api/public-api/get-saas-subscription/{locationId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["bulk-enable-saas-deprecated", {
    name: "bulk-enable-saas-deprecated",
    description: `Enable SaaS mode for multiple locations with support for both SaaS v1 and v2`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID to enable SaaS for"},"requestBody":{"type":"object","properties":{"locationIds":{"description":"Array of location IDs to enable SaaS for","type":"array","items":{"type":"string"}},"isSaaSV2":{"type":"boolean","description":"Indicates if the SaaS is V2"},"actionPayload":{"description":"Action payload for the bulk enable SaaS operation","allOf":[{"type":"object","properties":{"priceId":{"type":"string","description":"Price ID for the SaaS plan","example":"price_1QDPY5FpU9DlKp7RQ8BXfywx"},"stripeAccountId":{"type":"string","description":"Stripe account ID","example":"acct_1QDPY5FpU9DlKp7RQ8BXfywx"},"saasPlanId":{"type":"string","description":"SaaS plan ID","example":"66c4d36534f21f900dc2a265"},"providerLocationId":{"type":"string","description":"Provider location ID","example":"r06mdj4OrrERzYDvsOdh"}},"required":["saasPlanId"]}]}},"required":["locationIds","isSaaSV2","actionPayload"],"description":"The JSON request body."}},"required":["Authorization","Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas-api/public-api/bulk-enable-saas/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-saas-locations-deprecated", {
    name: "get-saas-locations-deprecated",
    description: `Fetch all SaaS-activated locations for a company with pagination`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID to get SaaS locations for"},"page":{"type":"number","description":"Page number for pagination"}},"required":["Authorization","Version","companyId"]},
    method: "get",
    pathTemplate: "/saas-api/public-api/saas-locations/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"},{"name":"page","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-saas-plan-deprecated", {
    name: "get-saas-plan-deprecated",
    description: `Fetch a specific SaaS plan by plan ID`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"planId":{"type":"string","description":"Plan ID to get SaaS plan details for"},"companyId":{"type":"string","description":"Company ID to filter SaaS plan"}},"required":["Authorization","Version","planId","companyId"]},
    method: "get",
    pathTemplate: "/saas-api/public-api/saas-plan/{planId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"planId","in":"path"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["locations", {
    name: "locations",
    description: `Get locations by stripeCustomerId or stripeSubscriptionId with companyId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"customerId":{"type":"string"},"subscriptionId":{"type":"string"},"companyId":{"type":"string"}},"required":["Authorization","Version","customerId","subscriptionId","companyId"]},
    method: "get",
    pathTemplate: "/saas/locations",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"customerId","in":"query"},{"name":"subscriptionId","in":"query"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["generate-payment-link", {
    name: "generate-payment-link",
    description: `Update SaaS subscription for given locationId and customerId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{"subscriptionId":{"type":"string","description":"Subscription ID"},"customerId":{"type":"string","description":"Customer ID"},"companyId":{"type":"string","description":"Company ID"}},"required":["subscriptionId","customerId","companyId"],"description":"The JSON request body."}},"required":["Authorization","Version","locationId","requestBody"]},
    method: "put",
    pathTemplate: "/saas/update-saas-subscription/{locationId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["bulk-disable-saas", {
    name: "bulk-disable-saas",
    description: `Disable SaaS for locations for given locationIds`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string"},"requestBody":{"type":"object","properties":{"locationIds":{"description":"Location IDs","type":"array","items":{"type":"string"}}},"required":["locationIds"],"description":"The JSON request body."}},"required":["Authorization","Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas/bulk-disable-saas/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["enable-saas-location", {
    name: "enable-saas-location",
    description: `<div>
                  <p>Enable SaaS for Sub-Account (Formerly Location) based on the data provided</p>
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                          This feature is only available on Agency Pro ($497) plan.
                        </strong>
                      </span>
                  </div>
                </div>
    `,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{"stripeAccountId":{"type":"string","description":"Stripe account id(Required only for SaaS V1)"},"name":{"type":"string","description":"Name of the stripe customer(Required only for SaaS V1)"},"email":{"type":"string","description":"Email of the stripe customer(Required only for SaaS V1)"},"stripeCustomerId":{"type":"string","description":"Stripe customer id if exists(Required only for SaaS V1)"},"companyId":{"type":"string"},"isSaaSV2":{"type":"boolean","description":"Denotes if it is a saas v2 or v1 sub-account"},"contactId":{"type":"string","description":"Agency subaccount used for payment provider integration(Required Only for SaaS V2)"},"providerLocationId":{"type":"string","description":"Agency Subaccount ID"},"description":{"type":"string","description":"Description"},"saasPlanId":{"type":"string","description":"Required only while pre-configuring saas subscription"},"priceId":{"type":"string","description":"Required only while pre-configuring saas subscription"}},"required":["companyId","isSaaSV2"],"description":"The JSON request body."}},"required":["Authorization","Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/saas/enable-saas/{locationId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["pause-location", {
    name: "pause-location",
    description: `Pause Sub account for given locationId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{"paused":{"type":"boolean","description":"Paused"},"companyId":{"type":"string","description":"Company ID"}},"required":["paused","companyId"],"description":"The JSON request body."}},"required":["Authorization","Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/saas/pause/{locationId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["update-rebilling", {
    name: "update-rebilling",
    description: `Bulk update rebilling for given locationIds`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string"},"requestBody":{"type":"object","properties":{"product":{"type":"string","description":"The product to update rebilling for","enum":["contentAI","workflow_premium_actions","workflow_ai","conversationAI","EmailNotification","whatsApp","reviewsAI","VERIFIED_CALLER_ID","WALLET_SALES_TAX","NOTIFICATION_SMS","EmailSmtp","EmailVerification","autoCompleteAddress","funnelAI","domainPurchase","Phone","Email"]},"locationIds":{"description":"Array of location IDs to update rebilling for","type":"array","items":{"type":"string"}},"config":{"type":"object","description":"Configuration for rebilling settings","properties":{"optIn":{"type":"boolean","description":"Enable the product for the locations"},"enabled":{"type":"boolean","description":"Enable the rebilling for the locations"},"markup":{"type":"number","description":"Additional value to be added in terms of percentage. For example, if the product price is $100 and the markup is 5, the amount charged to will be $105."}}}},"required":["product","locationIds","config"],"description":"The JSON request body."}},"required":["Authorization","Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas/update-rebilling/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-agency-plans", {
    name: "get-agency-plans",
    description: `Fetch all agency subscription plans for a given company ID`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string"}},"required":["Authorization","Version","companyId"]},
    method: "get",
    pathTemplate: "/saas/agency-plans/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-location-subscription", {
    name: "get-location-subscription",
    description: `Fetch subscription details for a specific location from location metadata`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string"},"companyId":{"type":"string"}},"required":["Authorization","Version","locationId","companyId"]},
    method: "get",
    pathTemplate: "/saas/get-saas-subscription/{locationId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["bulk-enable-saas", {
    name: "bulk-enable-saas",
    description: `Enable SaaS mode for multiple locations with support for both SaaS v1 and v2`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string"},"requestBody":{"type":"object","properties":{"locationIds":{"description":"Array of location IDs to enable SaaS for","type":"array","items":{"type":"string"}},"isSaaSV2":{"type":"boolean","description":"Indicates if the SaaS is V2"},"actionPayload":{"description":"Action payload for the bulk enable SaaS operation","allOf":[{"type":"object","properties":{"priceId":{"type":"string","description":"Price ID for the SaaS plan","example":"price_1QDPY5FpU9DlKp7RQ8BXfywx"},"stripeAccountId":{"type":"string","description":"Stripe account ID","example":"acct_1QDPY5FpU9DlKp7RQ8BXfywx"},"saasPlanId":{"type":"string","description":"SaaS plan ID","example":"66c4d36534f21f900dc2a265"},"providerLocationId":{"type":"string","description":"Provider location ID","example":"r06mdj4OrrERzYDvsOdh"}},"required":["saasPlanId"]}]}},"required":["locationIds","isSaaSV2","actionPayload"],"description":"The JSON request body."}},"required":["Authorization","Version","companyId","requestBody"]},
    method: "post",
    pathTemplate: "/saas/bulk-enable-saas/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-saas-locations", {
    name: "get-saas-locations",
    description: `Fetch all SaaS-activated locations for a company with pagination`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"companyId":{"type":"string"},"page":{"type":"number"}},"required":["Authorization","Version","companyId","page"]},
    method: "get",
    pathTemplate: "/saas/saas-locations/{companyId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"companyId","in":"path"},{"name":"page","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-saas-plan", {
    name: "get-saas-plan",
    description: `Fetch a specific SaaS plan by plan ID`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"planId":{"type":"string"},"companyId":{"type":"string"}},"required":["Authorization","Version","planId","companyId"]},
    method: "get",
    pathTemplate: "/saas/saas-plan/{planId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"planId","in":"path"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["create-snapshot-share-link", {
    name: "create-snapshot-share-link",
    description: `Create a share link for snapshot`,
    inputSchema: {"type":"object","properties":{"companyId":{"type":"string"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"snapshot_id":{"type":"string","description":"id for snapshot to be shared"},"share_type":{"type":"string","description":"Type of share link to generate","enum":["link","permanent_link","agency_link","location_link"]},"relationship_number":{"type":"string","description":"Comma separated Relationship number of Agencies to create agency restricted share link"},"share_location_id":{"type":"string","description":"Comma separated Sub-Account ids to create sub-account restricted share link"}},"required":["snapshot_id","share_type"],"description":"The JSON request body."}},"required":["companyId","Version","requestBody"]},
    method: "post",
    pathTemplate: "/snapshots/share/link",
    executionParameters: [{"name":"companyId","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-snapshot-push", {
    name: "get-snapshot-push",
    description: `Get list of sub-accounts snapshot pushed in time period`,
    inputSchema: {"type":"object","properties":{"snapshotId":{"type":"string"},"companyId":{"type":"string"},"from":{"type":"string"},"to":{"type":"string"},"lastDoc":{"type":"string","description":"Id for last document till what you want to skip"},"limit":{"type":"string"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["snapshotId","companyId","from","to","lastDoc","limit","Version"]},
    method: "get",
    pathTemplate: "/snapshots/snapshot-status/{snapshotId}",
    executionParameters: [{"name":"snapshotId","in":"path"},{"name":"companyId","in":"query"},{"name":"from","in":"query"},{"name":"to","in":"query"},{"name":"lastDoc","in":"query"},{"name":"limit","in":"query"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-latest-snapshot-push", {
    name: "get-latest-snapshot-push",
    description: `Get Latest Snapshot Push Status for a location id`,
    inputSchema: {"type":"object","properties":{"companyId":{"type":"string"},"snapshotId":{"type":"string"},"locationId":{"type":"string"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"}},"required":["companyId","snapshotId","locationId","Version"]},
    method: "get",
    pathTemplate: "/snapshots/snapshot-status/{snapshotId}/location/{locationId}",
    executionParameters: [{"name":"companyId","in":"query"},{"name":"snapshotId","in":"path"},{"name":"locationId","in":"path"},{"name":"Version","in":"header"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["start-google-oauth", {
    name: "start-google-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to Google login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "google" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch Google account details using below API -
  API: '/social-media-posting/oauth/google/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/google/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/oauth.readonly"]}]
  }],
  ["get-google-locations", {
    name: "get-google-locations",
    description: `Get google business locations`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/google/locations/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["set-google-locations", {
    name: "set-google-locations",
    description: `Set google business locations`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"location":{"type":"object"},"account":{"type":"object"},"companyId":{"type":"string","description":"Company ID"}},"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/google/locations/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-posts", {
    name: "get-posts",
    description: `Get Posts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"type":{"type":"string","description":"type must be one of the following values: recent, all, scheduled, draft, failed, in_review, published, in_progress and deleted","default":"all"},"accounts":{"type":"string","description":"List of account Ids seperated by comma as a string"},"skip":{"type":"string","default":"0"},"limit":{"type":"string","default":"10"},"fromDate":{"type":"string","description":"From Date"},"toDate":{"type":"string","description":"To Date"},"includeUsers":{"type":"string","description":"Include User Data"},"postType":{"type":"object","description":"Post Type must be one of the following values: - post, story, reel"}},"required":["skip","limit","fromDate","toDate","includeUsers"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/posts/list",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["socialplanner/post.readonly"]}]
  }],
  ["create-post", {
    name: "create-post",
    description: `Create posts for all supported platforms. It is possible to create customized posts per channel by using the same platform account IDs in a request and hitting the create post API multiple times with different summaries and account IDs per platform.

The content and media limitations, as well as platform rate limiters corresponding to the respective platforms, are provided in the following reference link:

  Link: [Platform Limitations](https://help.leadconnectorhq.com/support/solutions/articles/48001240003-social-planner-image-video-content-and-api-limitations "Social Planner Help")`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"accountIds":{"description":"Account Ids","type":"array","items":{"type":"string"}},"summary":{"type":"string","description":"Post Content \n The limitations of content as per the platforms is provided through the reference link in API description. The summary will be trimmed based on the limit"},"media":{"description":"Post Media Data \n The limitations of media as per the platforms is provided through the reference link in API description","type":"array","items":{"type":"object","properties":{"url":{"type":"string"},"caption":{"type":"string"},"type":{"type":"string"},"thumbnail":{"type":"string"},"defaultThumb":{"type":"string"},"id":{"type":"string"}},"required":["url"]}},"status":{"type":"object","description":"Status must be one of the following values: null, in_progress, draft, failed, published, scheduled, in_review, notification_sent, deleted"},"scheduleDate":{"type":"string","description":"Schedule Date"},"createdBy":{"type":"string","description":"Created By"},"followUpComment":{"type":"string","description":"Follow Up Comment on platform. It is not allowed on Tiktok and GMB accounts and there is a limit of 280 charecters for twitter account"},"ogTagsDetails":{"description":"Og Tags Meta Data","allOf":[{"type":"object","properties":{"metaImage":{"type":"string","description":"Meta Image","example":"https://wwww.lifeofanarchitect.com/wp-content/uploads/2021/10/architectural-model-making-stairs.jpg"},"metaLink":{"type":"string","description":"Meta Link"}}}]},"type":{"type":"object","description":"Post Type must be one of the following values: - post, story, reel"},"postApprovalDetails":{"description":"Post Approval Details","allOf":[{"type":"object","properties":{"approver":{"type":"string","description":"Approver","example":"iVrVJ2uoXNF0wzcBzgl5"},"requesterNote":{"type":"string","description":"Requester Notes","example":"Test suggestion"},"approverNote":{"type":"string","description":"Approver Notes","example":"Test suggestion"},"approvalStatus":{"type":"object","description":"Approval Status must be one of the following values: pending, approved, rejected, not_required","example":"approved"}}}]},"scheduleTimeUpdated":{"type":"boolean","description":"if schedule datetime is updated"},"tags":{"description":"Array of Tag Value","type":"array","items":{"type":"string"}},"categoryId":{"type":"string","description":"Category Id"},"tiktokPostDetails":{"description":"Tiktok Post Details","allOf":[{"type":"object","properties":{"privacyLevel":{"type":"object","description":"privacy level is an enum and must be one of the following values: PUBLIC_TO_EVERYONE, MUTUAL_FOLLOW_FRIENDS, SELF_ONLY","example":"PUBLIC_TO_EVERYONE"},"promoteOtherBrand":{"type":"boolean","description":"promote other brand","example":true},"enableComment":{"type":"boolean","description":"enable comment","example":true},"enableDuet":{"type":"boolean","description":"enable duet","example":true},"enableStitch":{"type":"boolean","description":"enable stitch","example":true},"videoDisclosure":{"type":"boolean","description":"video disclosure","example":true},"promoteYourBrand":{"type":"boolean","description":"promote your brand","example":true}}}]},"gmbPostDetails":{"description":"GMB Post Details","allOf":[{"type":"object","properties":{"gmbEventType":{"type":"string","example":"STANDARD","description":"gmbEventType must be one of the following values: STANDARD, EVENT, OFFER"},"title":{"type":"string","description":"Title","example":"Event Title"},"offerTitle":{"type":"string","description":"Offer Title","example":"Hey"},"startDate":{"description":"Start Date","allOf":[{"type":"object","properties":{"startDate":{"description":"Start Date","allOf":[{"type":"object","properties":{"year":{"type":"number","example":2022},"month":{"type":"number","example":1},"day":{"type":"number","example":1}},"required":["year","month","day"]}]},"startTime":{"description":"Start Time","allOf":[{"type":"object","properties":{"hours":{"type":"number","example":23},"minutes":{"type":"number","example":56},"seconds":{"type":"number","example":34}},"required":["hours","minutes","seconds"]}]}}}]},"endDate":{"description":"End Date","allOf":[{"type":"object","properties":{"endDate":{"description":"End Date","allOf":[{"type":"object","properties":{"year":{"type":"number","example":2022},"month":{"type":"number","example":1},"day":{"type":"number","example":1}},"required":["year","month","day"]}]},"endTime":{"description":"End Time","allOf":[{"type":"object","properties":{"hours":{"type":"number","example":23},"minutes":{"type":"number","example":56},"seconds":{"type":"number","example":34}},"required":["hours","minutes","seconds"]}]}}}]},"termsConditions":{"type":"string","description":"Terms Condition Url","example":"https://google.com/privacy"},"url":{"type":"string","description":"Url","example":"https://google.com"},"couponCode":{"type":"string","description":"Coupon Code","example":"BOGO-50"},"redeemOnlineUrl":{"type":"string","description":"Redeem Online Url","example":"https://google.com"},"actionType":{"type":"object","description":"Action Type must be one of the following values: none, order, book, shop, learn_more, call, sign_up","example":"book"}}}]},"userId":{"type":"string","description":"User ID"}},"required":["type","accountIds","userId"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/posts",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["socialplanner/post.write"]}]
  }],
  ["get-post", {
    name: "get-post",
    description: `Get post`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Post Id"}},"required":["Version","locationId","id"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/posts/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["edit-post", {
    name: "edit-post",
    description: `Create posts for all supported platforms. It is possible to create customized posts per channel by using the same platform account IDs in a request and hitting the create post API multiple times with different summaries and account IDs per platform.

The content and media limitations, as well as platform rate limiters corresponding to the respective platforms, are provided in the following reference link:

  Link: [Platform Limitations](https://help.leadconnectorhq.com/support/solutions/articles/48001240003-social-planner-image-video-content-and-api-limitations "Social Planner Help")`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Post Id"},"requestBody":{"type":"object","properties":{"accountIds":{"description":"Account Ids","type":"array","items":{"type":"string"}},"summary":{"type":"string","description":"Post Content \n The limitations of content as per the platforms is provided through the reference link in API description"},"media":{"description":"Post Media Data \n The limitations of media as per the platforms is provided through the reference link in API description","type":"array","items":{"type":"object","properties":{"url":{"type":"string"},"caption":{"type":"string"},"type":{"type":"string"},"thumbnail":{"type":"string"},"defaultThumb":{"type":"string"},"id":{"type":"string"}},"required":["url"]}},"status":{"type":"object","description":"Status must be one of the following values: in_progress, draft, failed, published, scheduled, in_review, notification_sent, deleted"},"scheduleDate":{"type":"string","description":"Schedule Date"},"createdBy":{"type":"string","description":"Created By"},"followUpComment":{"type":"string","description":"Follow Up Comment on platform. It is not allowed on Tiktok and GMB accounts and there is a limit of 280 charecters for twitter account"},"ogTagsDetails":{"description":"Og Tags Meta Data","allOf":[{"type":"object","properties":{"metaImage":{"type":"string","description":"Meta Image","example":"https://wwww.lifeofanarchitect.com/wp-content/uploads/2021/10/architectural-model-making-stairs.jpg"},"metaLink":{"type":"string","description":"Meta Link"}}}]},"type":{"type":"object","description":"Post Type must be one of the following values: - post, story, reel"},"postApprovalDetails":{"description":"Post Approval Details","allOf":[{"type":"object","properties":{"approver":{"type":"string","description":"Approver","example":"iVrVJ2uoXNF0wzcBzgl5"},"requesterNote":{"type":"string","description":"Requester Notes","example":"Test suggestion"},"approverNote":{"type":"string","description":"Approver Notes","example":"Test suggestion"},"approvalStatus":{"type":"object","description":"Approval Status must be one of the following values: pending, approved, rejected, not_required","example":"approved"}}}]},"scheduleTimeUpdated":{"type":"boolean","description":"if schedule datetime is updated"},"tags":{"description":"Array of Tag Value","type":"array","items":{"type":"string"}},"categoryId":{"type":"string","description":"Category Id"},"tiktokPostDetails":{"description":"Tiktok Post Details","allOf":[{"type":"object","properties":{"privacyLevel":{"type":"object","description":"privacy level is an enum and must be one of the following values: PUBLIC_TO_EVERYONE, MUTUAL_FOLLOW_FRIENDS, SELF_ONLY","example":"PUBLIC_TO_EVERYONE"},"promoteOtherBrand":{"type":"boolean","description":"promote other brand","example":true},"enableComment":{"type":"boolean","description":"enable comment","example":true},"enableDuet":{"type":"boolean","description":"enable duet","example":true},"enableStitch":{"type":"boolean","description":"enable stitch","example":true},"videoDisclosure":{"type":"boolean","description":"video disclosure","example":true},"promoteYourBrand":{"type":"boolean","description":"promote your brand","example":true}}}]},"gmbPostDetails":{"description":"GMB Post Details","allOf":[{"type":"object","properties":{"gmbEventType":{"type":"string","example":"STANDARD","description":"gmbEventType must be one of the following values: STANDARD, EVENT, OFFER"},"title":{"type":"string","description":"Title","example":"Event Title"},"offerTitle":{"type":"string","description":"Offer Title","example":"Hey"},"startDate":{"description":"Start Date","allOf":[{"type":"object","properties":{"startDate":{"description":"Start Date","allOf":[{"type":"object","properties":{"year":{"type":"number","example":2022},"month":{"type":"number","example":1},"day":{"type":"number","example":1}},"required":["year","month","day"]}]},"startTime":{"description":"Start Time","allOf":[{"type":"object","properties":{"hours":{"type":"number","example":23},"minutes":{"type":"number","example":56},"seconds":{"type":"number","example":34}},"required":["hours","minutes","seconds"]}]}}}]},"endDate":{"description":"End Date","allOf":[{"type":"object","properties":{"endDate":{"description":"End Date","allOf":[{"type":"object","properties":{"year":{"type":"number","example":2022},"month":{"type":"number","example":1},"day":{"type":"number","example":1}},"required":["year","month","day"]}]},"endTime":{"description":"End Time","allOf":[{"type":"object","properties":{"hours":{"type":"number","example":23},"minutes":{"type":"number","example":56},"seconds":{"type":"number","example":34}},"required":["hours","minutes","seconds"]}]}}}]},"termsConditions":{"type":"string","description":"Terms Condition Url","example":"https://google.com/privacy"},"url":{"type":"string","description":"Url","example":"https://google.com"},"couponCode":{"type":"string","description":"Coupon Code","example":"BOGO-50"},"redeemOnlineUrl":{"type":"string","description":"Redeem Online Url","example":"https://google.com"},"actionType":{"type":"object","description":"Action Type must be one of the following values: none, order, book, shop, learn_more, call, sign_up","example":"book"}}}]},"userId":{"type":"string","description":"User ID"}},"required":["type"],"description":"The JSON request body."}},"required":["Version","locationId","id","requestBody"]},
    method: "put",
    pathTemplate: "/social-media-posting/{locationId}/posts/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-post", {
    name: "delete-post",
    description: `Delete Post`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Post Id"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/social-media-posting/{locationId}/posts/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["bulk-delete-social-planner-posts", {
    name: "bulk-delete-social-planner-posts",
    description: `Deletes multiple posts based on the provided list of post IDs. 
                  This operation is useful for clearing up large numbers of posts efficiently. 
                  
Note: 
                  
1.The maximum number of posts that can be deleted in a single request is '50'.
                  
2.However, It will only get deleted in Highlevel database but still
                   it is recommended to be cautious of this operation.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"requestBody":{"type":"object","properties":{"postIds":{"description":"Requested Results","type":"array","items":{"type":"string"}}},"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/posts/bulk-delete",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["get-account", {
    name: "get-account",
    description: `Get list of accounts and groups`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/accounts",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/account.readonly"]}]
  }],
  ["delete-account", {
    name: "delete-account",
    description: `Delete account and account from group`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"id":{"type":"string","description":"Id"},"companyId":{"type":"string","description":"Company ID"},"userId":{"type":"string","description":"User ID"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/social-media-posting/{locationId}/accounts/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"},{"name":"companyId","in":"query"},{"name":"userId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-facebook-oauth", {
    name: "start-facebook-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to Facebook login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "facebook" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch Facebook account details using below API -
  API: '/social-media-posting/oauth/facebook/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"userId":{"type":"string","description":"User ID"},"page":{"type":"string","description":"Facebook Page"},"reconnect":{"type":"string","description":"Reconnect boolean as string"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/facebook/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-facebook-page-group", {
    name: "get-facebook-page-group",
    description: `Get facebook pages`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/facebook/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["attach-facebook-page-group", {
    name: "attach-facebook-page-group",
    description: `Attach facebook pages`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"type":{"type":"object"},"originId":{"type":"string"},"name":{"type":"string"},"avatar":{"type":"string"},"companyId":{"type":"string","description":"Company ID"}},"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/facebook/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-instagram-oauth", {
    name: "start-instagram-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to Instagram login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "instagram" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch Instagram account details using below API -
  API: '/social-media-posting/oauth/instagram/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/instagram/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/oauth.readonly"]}]
  }],
  ["get-instagram-page-group", {
    name: "get-instagram-page-group",
    description: `Get Instagram Professional Accounts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/instagram/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["attach-instagram-page-group", {
    name: "attach-instagram-page-group",
    description: `Attach Instagram Professional Accounts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"originId":{"type":"string"},"name":{"type":"string"},"avatar":{"type":"string"},"pageId":{"type":"string"},"companyId":{"type":"string","description":"Company ID"}},"required":["pageId"],"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/instagram/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-linkedin-oauth", {
    name: "start-linkedin-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to LinkedIn login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "linkedin" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch LinkedIn account details using below API -
  API: '/social-media-posting/oauth/linkedin/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/linkedin/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-linkedin-page-profile", {
    name: "get-linkedin-page-profile",
    description: `Get Linkedin pages and profile`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/linkedin/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["attach-linkedin-page-profile", {
    name: "attach-linkedin-page-profile",
    description: `Attach linkedin pages and profile`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"type":{"type":"string","enum":["page","group","profile","location","business"]},"originId":{"type":"string"},"name":{"type":"string"},"avatar":{"type":"string"},"urn":{"type":"string"},"companyId":{"type":"string","description":"Company ID"}},"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/linkedin/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-twitter-oauth", {
    name: "start-twitter-oauth",
    description: `<div><div>
  <span style= "display: inline-block;
    width: 25px; height: 25px;
    background-color: red;
    color: black;
    font-weight: bold;
    font-size: 24px;
    text-align: center;
    line-height: 20px;
    border: 2px solid black;
    border-radius: 20%;
    margin-right: 10px;">
    !
  </span>
  <span><strong>As of December 4, 2024, X (formerly Twitter) is no longer supported. We apologise for any inconvenience.</strong></span>
</div></div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/twitter/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/oauth.readonly"]}]
  }],
  ["get-twitter-profile", {
    name: "get-twitter-profile",
    description: `<div><div>
  <span style= "display: inline-block;
    width: 25px; height: 25px;
    background-color: red;
    color: black;
    font-weight: bold;
    font-size: 24px;
    text-align: center;
    line-height: 20px;
    border: 2px solid black;
    border-radius: 20%;
    margin-right: 10px;">
    !
  </span>
  <span><strong>As of December 4, 2024, X (formerly Twitter) is no longer supported. We apologise for any inconvenience.</strong></span>
</div></div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/twitter/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["attach-twitter-profile", {
    name: "attach-twitter-profile",
    description: `<div><div>
  <span style= "display: inline-block;
    width: 25px; height: 25px;
    background-color: red;
    color: black;
    font-weight: bold;
    font-size: 24px;
    text-align: center;
    line-height: 20px;
    border: 2px solid black;
    border-radius: 20%;
    margin-right: 10px;">
    !
  </span>
  <span><strong>As of December 4, 2024, X (formerly Twitter) is no longer supported. We apologise for any inconvenience.</strong></span>
</div></div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"originId":{"type":"string"},"name":{"type":"string"},"username":{"type":"string"},"avatar":{"type":"string"},"protected":{"type":"boolean"},"verified":{"type":"boolean"},"companyId":{"type":"string","description":"Company ID"}},"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/twitter/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["get-upload-status", {
    name: "get-upload-status",
    description: `Get Upload Status`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"skip":{"default":"0","type":"string"},"limit":{"default":"10","type":"string"},"includeUsers":{"type":"string"},"userId":{"type":"string","description":"User ID"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/csv",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"includeUsers","in":"query"},{"name":"userId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["upload-csv", {
    name: "upload-csv",
    description: `Upload CSV`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"string","description":"Request body (content type: multipart/form-data)"}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/csv",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "multipart/form-data",
    securityRequirements: [{"bearer":["socialplanner/csv.write"]}]
  }],
  ["set-accounts", {
    name: "set-accounts",
    description: `Set Accounts`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"accountIds":{"description":"Account Ids","type":"array","items":{"type":"string"}},"filePath":{"type":"string","description":"File path"},"rowsCount":{"type":"number","description":"Entires Count. rowcCount must be between 1 and number of posts in CSV"},"fileName":{"type":"string","description":"Name of file"},"approver":{"type":"string"},"userId":{"type":"string","description":"User ID"}},"required":["accountIds","filePath","rowsCount","fileName"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/set-accounts",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["socialplanner/csv.write"]}]
  }],
  ["get-csv-post", {
    name: "get-csv-post",
    description: `Get CSV Post`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"id":{"type":"string","description":"CSV Id"},"skip":{"type":"string"},"limit":{"type":"string"}},"required":["Version","locationId","id"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/csv/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"},{"name":"skip","in":"query"},{"name":"limit","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-csv", {
    name: "delete-csv",
    description: `Delete CSV`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"id":{"type":"string","description":"CSV Id"}},"required":["Version","locationId","id"]},
    method: "delete",
    pathTemplate: "/social-media-posting/{locationId}/csv/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-csv-finalize", {
    name: "start-csv-finalize",
    description: `Start CSV Finalize`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"id":{"type":"string","description":"CSV Id"},"requestBody":{"type":"object","properties":{"userId":{"type":"string","description":"User ID"}},"description":"The JSON request body."}},"required":["Version","locationId","id","requestBody"]},
    method: "patch",
    pathTemplate: "/social-media-posting/{locationId}/csv/{id}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"id","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["delete-csv-post", {
    name: "delete-csv-post",
    description: `Delete CSV Post`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"postId":{"type":"string","description":"CSV Post Id"},"csvId":{"type":"string","description":"CSV Id"}},"required":["Version","locationId","postId","csvId"]},
    method: "delete",
    pathTemplate: "/social-media-posting/{locationId}/csv/{csvId}/post/{postId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"postId","in":"path"},{"name":"csvId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-tiktok-oauth", {
    name: "start-tiktok-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to Tiktok login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "tiktok" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch Tiktok account details using below API -
  API: '/social-media-posting/oauth/tiktok/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/tiktok/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/oauth.readonly"]}]
  }],
  ["get-tiktok-profile", {
    name: "get-tiktok-profile",
    description: `Get Tiktok profile`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/tiktok/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["attach-tiktok-profile", {
    name: "attach-tiktok-profile",
    description: `Attach Tiktok profile`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"},"requestBody":{"type":"object","properties":{"type":{"type":"string","enum":["page","group","profile","location","business"]},"originId":{"type":"string"},"name":{"type":"string"},"avatar":{"type":"string"},"verified":{"type":"boolean"},"username":{"type":"string"},"companyId":{"type":"string","description":"Company ID"}},"description":"The JSON request body."}},"required":["Version","locationId","accountId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/oauth/{locationId}/tiktok/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["start-tiktok-business-oauth", {
    name: "start-tiktok-business-oauth",
    description: `Open the API in a window with appropriate params and headers instead of using the Curl. User is navigated to Tiktok-Business login OAuth screen. On successful login, listen on window object for message where event listener returns data in its callback function. 
  ### Sample code to listen to event data:
    window.addEventListener('message', 
      function(e) {
        if (e.data && e.data.page === 'social_media_posting') {
        const { actionType, page, platform, placement, accountId, reconnectAccounts } = e.data
        }
      },
    false)
  ### Event Data Response:
    {
      actionType: string,            Ex: "close" 
      page: string,                  Ex: "social-media-posting" 
      platform: string,              Ex: "tiktok-business" 
      placement: string,             Ex: "placement" 
      accountId: string,             Ex: "658a9b6833b91e0ecb8f3958" 
      reconnectAccounts: string[]]   Ex: ["658a9b6833b91e0ecb834acd", "efd2daa9b6833b91e0ecb8f3511"] 
    }
  ### The accountId retrieved from above data can be used to fetch Tiktok-Business account details using below API -
  API: '/social-media-posting/oauth/tiktok-business/accounts/:accountId' 

  Method: GET`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"userId":{"type":"string","description":"User Id"},"page":{"type":"string","description":"Page"},"reconnect":{"type":"string","description":"Reconnect"}},"required":["Version","locationId","userId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/tiktok-business/start",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"userId","in":"query"},{"name":"page","in":"query"},{"name":"reconnect","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/oauth.readonly"]}]
  }],
  ["get-tiktok-business-profile", {
    name: "get-tiktok-business-profile",
    description: `Get Tiktok Business profile`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Account Location Id"},"accountId":{"type":"string","description":"Account Id"}},"required":["Version","locationId","accountId"]},
    method: "get",
    pathTemplate: "/social-media-posting/oauth/{locationId}/tiktok-business/accounts/{accountId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"accountId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-categories-location-id", {
    name: "get-categories-location-id",
    description: `Get categories by location id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"searchText":{"type":"string","description":"Search text string"},"limit":{"type":"string","description":"Limit"},"skip":{"type":"string","description":"Skip"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/categories",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"searchText","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["socialplanner/category.readonly"]}]
  }],
  ["get-categories-id", {
    name: "get-categories-id",
    description: `Get categories by id`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"id":{"type":"string","description":"Category Id"},"locationId":{"type":"string","description":"Location Id"}},"required":["Authorization","Version","id","locationId"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/categories/{id}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"Version","in":"header"},{"name":"id","in":"path"},{"name":"locationId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-tags-location-id", {
    name: "get-tags-location-id",
    description: `Get tags by location id`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"searchText":{"type":"string","description":"Search text string"},"limit":{"type":"string","description":"Limit"},"skip":{"type":"string","description":"Skip"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/social-media-posting/{locationId}/tags",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"},{"name":"searchText","in":"query"},{"name":"limit","in":"query"},{"name":"skip","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-tags-by-ids", {
    name: "get-tags-by-ids",
    description: `Get tags by ids`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"requestBody":{"type":"object","properties":{"tagIds":{"description":"Array of Tag Ids","type":"array","items":{"type":"string"}}},"required":["tagIds"],"description":"The JSON request body."}},"required":["Version","locationId","requestBody"]},
    method: "post",
    pathTemplate: "/social-media-posting/{locationId}/tags/details",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":[]}]
  }],
  ["get-social-media-statistics", {
    name: "get-social-media-statistics",
    description: `Retrieve analytics data for multiple social media accounts. Provides metrics for the last 7 days with comparison to the previous 7 days. Supports filtering by platforms and specific connected accounts.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location ID"},"requestBody":{"type":"object","properties":{"profileIds":{"type":"array","description":"Array of connected social media account IDs to fetch analytics for. This can be found as 'profileId' in /accounts api.","items":{"type":"string"},"minItems":1},"platforms":{"type":"array","description":"Array of social media platforms to filter analytics by. If not provided, all platforms will be included. NOTE: Linkedin (PAGE only) and Tiktok (BUSINESS only) are supported.","items":{"type":"string","enum":["facebook","instagram","linkedin","google","pinterest","youtube","tiktok"]}}},"required":["profileIds"],"description":"The JSON request body."}},"required":["Version","locationId"]},
    method: "post",
    pathTemplate: "/social-media-posting/statistics",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["list-shipping-zones", {
    name: "list-shipping-zones",
    description: `The "List Shipping Zone" API allows to retrieve a list of shipping zone.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"withShippingRate":{"type":"boolean","description":"Include shipping rates array"}},"required":["Authorization","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-zone",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"withShippingRate","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["create-shipping-zone", {
    name: "create-shipping-zone",
    description: `The "Create Shipping Zone" API allows adding a new shipping zone.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping zone"},"countries":{"description":"List of countries that are available","type":"array","items":{"type":"object","properties":{"code":{"type":"string","description":"Country code","enum":["US","CA","AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","KR","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"states":{"description":"List of states that are available. If states is empty, then all states are available","type":"array","items":{"type":"object","properties":{"code":{"type":"string","description":"State code","enum":["AL","AK","AS","AZ","AR","AA","AE","AP","CA","CO","CT","DE","DC","FM","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MH","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY","AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT","BA","CT","CC","CH","CB","CN","ER","FO","JY","LP","LR","MZ","MN","NQ","RN","SA","SJ","SL","SC","SF","SE","TF","TU","ACT","NSW","NT","QLD","SA","TAS","VIC","WA","AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO","AI","AN","AP","AT","BI","CO","AR","LI","LL","LR","MA","ML","RM","TA","VS","NB","AMA","ANT","ARA","ATL","BOL","BOY","CAL","CAQ","CAS","CAU","CES","CHO","CUN","COR","GUA","GUV","HUI","LAG","MAG","MET","NAR","NSA","PUT","QUI","RIS","SAP","SAN","SUC","TOL","VAC","VAU","VID","CR-A","CR-C","CR-G","CR-H","CR-L","CR-P","CR-SJ","GT-16","GT-15","GT-04","GT-20","GT-02","GT-05","GT-01","GT-13","GT-18","GT-21","GT-22","GT-17","GT-09","GT-14","GT-11","GT-03","GT-12","GT-06","GT-07","GT-10","GT-08","GT-19","HK","KL","NT","AN","AP","AR","AS","BR","CH","CG","DN","DD","DL","GA","GJ","HR","HP","JK","JH","KA","KL","LA","LD","MP","MH","MN","ML","MZ","NL","OR","PY","PB","RJ","SK","TN","TS","TR","UP","UK","WB","CW","CN","CE","CO","DL","D","G","KY","KE","KK","LS","LM","LK","LD","LH","MO","MH","MN","OY","RN","SO","TA","WD","WH","WX","WW","AG","AL","AN","AO","AR","AP","AT","AV","BA","BT","BL","BN","BG","BI","BO","BZ","BS","BR","CA","CL","CB","CI","CE","CT","CZ","CH","CO","CS","CR","KR","CN","EN","FM","FE","FI","FG","FC","FR","GE","GO","GR","IM","IS","AQ","SP","LT","LE","LC","LI","LO","LU","MC","MN","MS","MT","VS","ME","MI","MO","MB","NA","NO","NU","OG","OT","OR","PD","PA","PR","PV","PG","PU","PE","PC","PI","PT","PN","PZ","PO","RG","RA","RC","RE","RI","RN","RM","RO","SA","SS","SV","SI","SR","SO","TA","TE","TR","TO","TP","TN","TV","TS","UD","VA","VE","VB","VC","VR","VV","VI","VT","JP-23","JP-05","JP-02","JP-12","JP-38","JP-18","JP-40","JP-07","JP-21","JP-10","JP-34","JP-01","JP-28","JP-08","JP-17","JP-03","JP-37","JP-46","JP-14","JP-39","JP-43","JP-26","JP-24","JP-04","JP-45","JP-20","JP-42","JP-29","JP-15","JP-44","JP-33","JP-47","JP-27","JP-41","JP-11","JP-25","JP-32","JP-22","JP-09","JP-36","JP-13","JP-31","JP-16","JP-30","JP-06","JP-35","JP-19","JHR","KDH","KTN","KUL","LBN","MLK","NSN","PHG","PNG","PRK","PLS","PJY","SBH","SWK","SGR","TRG","AGU","BCN","BCS","CAM","CHP","CHH","CMX","COA","COL","DUR","GUA","GRO","HID","JAL","MIC","MOR","MEX","NAY","NLE","OAX","PUE","QUE","ROO","SLP","SIN","SON","TAB","TAM","TLA","VER","YUC","ZAC","AUK","BOP","CAN","CIT","GIS","HKB","MWT","MBH","NSN","NTL","OTA","STL","TKI","TAS","WKO","WGN","WTC","JK","BA","GB","IS","KP","PB","SD","AMA","ANC","APU","ARE","AYA","CAJ","CAL","CUS","HUV","HUC","ICA","JUN","LAL","LAM","LIM","LOR","MDD","MOQ","PAS","PIU","PUN","SAM","TAC","TUM","UCA","PH-ABR","PH-AGN","PH-AGS","PH-AKL","PH-ALB","PH-ANT","PH-APA","PH-AUR","PH-BAS","PH-BAN","PH-BTN","PH-BTG","PH-BEN","PH-BIL","PH-BOH","PH-BUK","PH-BUL","PH-CAG","PH-CAN","PH-CAS","PH-CAM","PH-CAP","PH-CAT","PH-CAV","PH-CEB","PH-NCO","PH-DAO","PH-DAV","PH-DAS","PH-EAS","PH-GUI","PH-IFU","PH-ILN","PH-ILS","PH-ILI","PH-ISA","PH-KAL","PH-LUN","PH-LAG","PH-LAN","PH-LAS","PH-LEY","PH-MAG","PH-MAD","PH-MAS","PH-00","PH-MSC","PH-MSR","PH-MOU","PH-NEC","PH-NER","PH-NSA","PH-NUE","PH-NUV","PH-MDC","PH-MDR","PH-PLW","PH-PAM","PH-PAN","PH-QUE","PH-QUI","PH-RIZ","PH-ROM","PH-WSA","PH-SAR","PH-SIG","PH-SOR","PH-SCO","PH-SLE","PH-SUK","PH-SLU","PH-SUN","PH-SUR","PH-TAR","PH-TAW","PH-ZMB","PH-ZAN","PH-ZAS","PH-ZSI","PT-20","PT-01","PT-02","PT-03","PT-04","PT-05","PT-06","PT-07","PT-08","PT-09","PT-10","PT-11","PT-30","PT-12","PT-13","PT-14","PT-15","PT-16","PT-17","PT-18","AB","AR","AG","BC","BH","BN","BT","BR","BV","B","BZ","CL","CS","CJ","CT","CV","DB","DJ","GL","GR","GJ","HR","HD","IL","IS","IF","MM","MH","MS","NT","OT","PH","SJ","SM","SB","SV","TR","TM","TL","VL","VS","VN","KR-26","KR-43","KR-44","KR-27","KR-30","KR-42","KR-29","KR-47","KR-41","KR-48","KR-28","KR-49","KR-45","KR-46","KR-50","KR-11","KR-31","C","VI","AB","A","AL","O","AV","BA","PM","B","BU","CC","CA","S","CS","CE","CR","CO","CU","GI","GR","GU","SS","H","HU","J","LO","GC","LE","L","LU","M","MA","ML","MU","NA","OR","P","PO","SA","TF","SG","SE","SO","T","TE","TO","V","VA","BI","ZA","Z","AZ","AJ","DU","FU","RK","SH","UQ","BFP","ENG","NIR","SCT","WLS","AR","CA","CL","CO","DU","FS","FD","LA","MA","MO","PA","RN","RV","RO","SA","SJ","SO","TA","TT"]}},"required":["code"]}}},"required":["code"]}}},"required":["altId","altType","name","countries"],"description":"The JSON request body."}},"required":["Authorization","requestBody"]},
    method: "post",
    pathTemplate: "/store/shipping-zone",
    executionParameters: [{"name":"Authorization","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-shipping-zones", {
    name: "get-shipping-zones",
    description: `The "List Shipping Zone" API allows to retrieve a paginated list of shipping zone.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the item that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"withShippingRate":{"type":"boolean","description":"Include shipping rates array"}},"required":["Authorization","shippingZoneId","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"withShippingRate","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["update-shipping-zone", {
    name: "update-shipping-zone",
    description: `The "update Shipping Zone" API allows update a shipping zone to the system. `,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the item that needs to be returned"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping zone"},"countries":{"description":"List of countries that are available","type":"array","items":{"type":"object","properties":{"code":{"type":"string","description":"Country code","enum":["US","CA","AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","KR","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"states":{"description":"List of states that are available. If states is empty, then all states are available","type":"array","items":{"type":"object","properties":{"code":{"type":"string","description":"State code","enum":["AL","AK","AS","AZ","AR","AA","AE","AP","CA","CO","CT","DE","DC","FM","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MH","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY","AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT","BA","CT","CC","CH","CB","CN","ER","FO","JY","LP","LR","MZ","MN","NQ","RN","SA","SJ","SL","SC","SF","SE","TF","TU","ACT","NSW","NT","QLD","SA","TAS","VIC","WA","AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO","AI","AN","AP","AT","BI","CO","AR","LI","LL","LR","MA","ML","RM","TA","VS","NB","AMA","ANT","ARA","ATL","BOL","BOY","CAL","CAQ","CAS","CAU","CES","CHO","CUN","COR","GUA","GUV","HUI","LAG","MAG","MET","NAR","NSA","PUT","QUI","RIS","SAP","SAN","SUC","TOL","VAC","VAU","VID","CR-A","CR-C","CR-G","CR-H","CR-L","CR-P","CR-SJ","GT-16","GT-15","GT-04","GT-20","GT-02","GT-05","GT-01","GT-13","GT-18","GT-21","GT-22","GT-17","GT-09","GT-14","GT-11","GT-03","GT-12","GT-06","GT-07","GT-10","GT-08","GT-19","HK","KL","NT","AN","AP","AR","AS","BR","CH","CG","DN","DD","DL","GA","GJ","HR","HP","JK","JH","KA","KL","LA","LD","MP","MH","MN","ML","MZ","NL","OR","PY","PB","RJ","SK","TN","TS","TR","UP","UK","WB","CW","CN","CE","CO","DL","D","G","KY","KE","KK","LS","LM","LK","LD","LH","MO","MH","MN","OY","RN","SO","TA","WD","WH","WX","WW","AG","AL","AN","AO","AR","AP","AT","AV","BA","BT","BL","BN","BG","BI","BO","BZ","BS","BR","CA","CL","CB","CI","CE","CT","CZ","CH","CO","CS","CR","KR","CN","EN","FM","FE","FI","FG","FC","FR","GE","GO","GR","IM","IS","AQ","SP","LT","LE","LC","LI","LO","LU","MC","MN","MS","MT","VS","ME","MI","MO","MB","NA","NO","NU","OG","OT","OR","PD","PA","PR","PV","PG","PU","PE","PC","PI","PT","PN","PZ","PO","RG","RA","RC","RE","RI","RN","RM","RO","SA","SS","SV","SI","SR","SO","TA","TE","TR","TO","TP","TN","TV","TS","UD","VA","VE","VB","VC","VR","VV","VI","VT","JP-23","JP-05","JP-02","JP-12","JP-38","JP-18","JP-40","JP-07","JP-21","JP-10","JP-34","JP-01","JP-28","JP-08","JP-17","JP-03","JP-37","JP-46","JP-14","JP-39","JP-43","JP-26","JP-24","JP-04","JP-45","JP-20","JP-42","JP-29","JP-15","JP-44","JP-33","JP-47","JP-27","JP-41","JP-11","JP-25","JP-32","JP-22","JP-09","JP-36","JP-13","JP-31","JP-16","JP-30","JP-06","JP-35","JP-19","JHR","KDH","KTN","KUL","LBN","MLK","NSN","PHG","PNG","PRK","PLS","PJY","SBH","SWK","SGR","TRG","AGU","BCN","BCS","CAM","CHP","CHH","CMX","COA","COL","DUR","GUA","GRO","HID","JAL","MIC","MOR","MEX","NAY","NLE","OAX","PUE","QUE","ROO","SLP","SIN","SON","TAB","TAM","TLA","VER","YUC","ZAC","AUK","BOP","CAN","CIT","GIS","HKB","MWT","MBH","NSN","NTL","OTA","STL","TKI","TAS","WKO","WGN","WTC","JK","BA","GB","IS","KP","PB","SD","AMA","ANC","APU","ARE","AYA","CAJ","CAL","CUS","HUV","HUC","ICA","JUN","LAL","LAM","LIM","LOR","MDD","MOQ","PAS","PIU","PUN","SAM","TAC","TUM","UCA","PH-ABR","PH-AGN","PH-AGS","PH-AKL","PH-ALB","PH-ANT","PH-APA","PH-AUR","PH-BAS","PH-BAN","PH-BTN","PH-BTG","PH-BEN","PH-BIL","PH-BOH","PH-BUK","PH-BUL","PH-CAG","PH-CAN","PH-CAS","PH-CAM","PH-CAP","PH-CAT","PH-CAV","PH-CEB","PH-NCO","PH-DAO","PH-DAV","PH-DAS","PH-EAS","PH-GUI","PH-IFU","PH-ILN","PH-ILS","PH-ILI","PH-ISA","PH-KAL","PH-LUN","PH-LAG","PH-LAN","PH-LAS","PH-LEY","PH-MAG","PH-MAD","PH-MAS","PH-00","PH-MSC","PH-MSR","PH-MOU","PH-NEC","PH-NER","PH-NSA","PH-NUE","PH-NUV","PH-MDC","PH-MDR","PH-PLW","PH-PAM","PH-PAN","PH-QUE","PH-QUI","PH-RIZ","PH-ROM","PH-WSA","PH-SAR","PH-SIG","PH-SOR","PH-SCO","PH-SLE","PH-SUK","PH-SLU","PH-SUN","PH-SUR","PH-TAR","PH-TAW","PH-ZMB","PH-ZAN","PH-ZAS","PH-ZSI","PT-20","PT-01","PT-02","PT-03","PT-04","PT-05","PT-06","PT-07","PT-08","PT-09","PT-10","PT-11","PT-30","PT-12","PT-13","PT-14","PT-15","PT-16","PT-17","PT-18","AB","AR","AG","BC","BH","BN","BT","BR","BV","B","BZ","CL","CS","CJ","CT","CV","DB","DJ","GL","GR","GJ","HR","HD","IL","IS","IF","MM","MH","MS","NT","OT","PH","SJ","SM","SB","SV","TR","TM","TL","VL","VS","VN","KR-26","KR-43","KR-44","KR-27","KR-30","KR-42","KR-29","KR-47","KR-41","KR-48","KR-28","KR-49","KR-45","KR-46","KR-50","KR-11","KR-31","C","VI","AB","A","AL","O","AV","BA","PM","B","BU","CC","CA","S","CS","CE","CR","CO","CU","GI","GR","GU","SS","H","HU","J","LO","GC","LE","L","LU","M","MA","ML","MU","NA","OR","P","PO","SA","TF","SG","SE","SO","T","TE","TO","V","VA","BI","ZA","Z","AZ","AJ","DU","FU","RK","SH","UQ","BFP","ENG","NIR","SCT","WLS","AR","CA","CL","CO","DU","FS","FD","LA","MA","MO","PA","RN","RV","RO","SA","SJ","SO","TA","TT"]}},"required":["code"]}}},"required":["code"]}}},"description":"The JSON request body."}},"required":["Authorization","shippingZoneId","requestBody"]},
    method: "put",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["delete-shipping-zone", {
    name: "delete-shipping-zone",
    description: `Delete specific shipping zone with Id :shippingZoneId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the item that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","shippingZoneId","altId","altType"]},
    method: "delete",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-available-shipping-zones", {
    name: "get-available-shipping-zones",
    description: `This return available shipping rates for country based on order amount`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"country":{"type":"string","description":"Country code of the customer","enum":["US","CA","AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","KR","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"address":{"description":"Address of the customer","allOf":[{"type":"object","properties":{"name":{"type":"string","description":"Name of the customer","example":"John Doe"},"companyName":{"type":"string","description":"Name of the Company","example":"ABC Company"},"addressLine1":{"type":"string","description":"Address line 1 of the customer","example":"123 Main St."},"country":{"type":"string","description":"Country code of the customer","enum":["US","CA","AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","KR","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"],"example":"US"},"state":{"type":"string","description":"State code of the customer","enum":["AL","AK","AS","AZ","AR","AA","AE","AP","CA","CO","CT","DE","DC","FM","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MH","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY","AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT","BA","CT","CC","CH","CB","CN","ER","FO","JY","LP","LR","MZ","MN","NQ","RN","SA","SJ","SL","SC","SF","SE","TF","TU","ACT","NSW","NT","QLD","SA","TAS","VIC","WA","AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO","AI","AN","AP","AT","BI","CO","AR","LI","LL","LR","MA","ML","RM","TA","VS","NB","AMA","ANT","ARA","ATL","BOL","BOY","CAL","CAQ","CAS","CAU","CES","CHO","CUN","COR","GUA","GUV","HUI","LAG","MAG","MET","NAR","NSA","PUT","QUI","RIS","SAP","SAN","SUC","TOL","VAC","VAU","VID","CR-A","CR-C","CR-G","CR-H","CR-L","CR-P","CR-SJ","GT-16","GT-15","GT-04","GT-20","GT-02","GT-05","GT-01","GT-13","GT-18","GT-21","GT-22","GT-17","GT-09","GT-14","GT-11","GT-03","GT-12","GT-06","GT-07","GT-10","GT-08","GT-19","HK","KL","NT","AN","AP","AR","AS","BR","CH","CG","DN","DD","DL","GA","GJ","HR","HP","JK","JH","KA","KL","LA","LD","MP","MH","MN","ML","MZ","NL","OR","PY","PB","RJ","SK","TN","TS","TR","UP","UK","WB","CW","CN","CE","CO","DL","D","G","KY","KE","KK","LS","LM","LK","LD","LH","MO","MH","MN","OY","RN","SO","TA","WD","WH","WX","WW","AG","AL","AN","AO","AR","AP","AT","AV","BA","BT","BL","BN","BG","BI","BO","BZ","BS","BR","CA","CL","CB","CI","CE","CT","CZ","CH","CO","CS","CR","KR","CN","EN","FM","FE","FI","FG","FC","FR","GE","GO","GR","IM","IS","AQ","SP","LT","LE","LC","LI","LO","LU","MC","MN","MS","MT","VS","ME","MI","MO","MB","NA","NO","NU","OG","OT","OR","PD","PA","PR","PV","PG","PU","PE","PC","PI","PT","PN","PZ","PO","RG","RA","RC","RE","RI","RN","RM","RO","SA","SS","SV","SI","SR","SO","TA","TE","TR","TO","TP","TN","TV","TS","UD","VA","VE","VB","VC","VR","VV","VI","VT","JP-23","JP-05","JP-02","JP-12","JP-38","JP-18","JP-40","JP-07","JP-21","JP-10","JP-34","JP-01","JP-28","JP-08","JP-17","JP-03","JP-37","JP-46","JP-14","JP-39","JP-43","JP-26","JP-24","JP-04","JP-45","JP-20","JP-42","JP-29","JP-15","JP-44","JP-33","JP-47","JP-27","JP-41","JP-11","JP-25","JP-32","JP-22","JP-09","JP-36","JP-13","JP-31","JP-16","JP-30","JP-06","JP-35","JP-19","JHR","KDH","KTN","KUL","LBN","MLK","NSN","PHG","PNG","PRK","PLS","PJY","SBH","SWK","SGR","TRG","AGU","BCN","BCS","CAM","CHP","CHH","CMX","COA","COL","DUR","GUA","GRO","HID","JAL","MIC","MOR","MEX","NAY","NLE","OAX","PUE","QUE","ROO","SLP","SIN","SON","TAB","TAM","TLA","VER","YUC","ZAC","AUK","BOP","CAN","CIT","GIS","HKB","MWT","MBH","NSN","NTL","OTA","STL","TKI","TAS","WKO","WGN","WTC","JK","BA","GB","IS","KP","PB","SD","AMA","ANC","APU","ARE","AYA","CAJ","CAL","CUS","HUV","HUC","ICA","JUN","LAL","LAM","LIM","LOR","MDD","MOQ","PAS","PIU","PUN","SAM","TAC","TUM","UCA","PH-ABR","PH-AGN","PH-AGS","PH-AKL","PH-ALB","PH-ANT","PH-APA","PH-AUR","PH-BAS","PH-BAN","PH-BTN","PH-BTG","PH-BEN","PH-BIL","PH-BOH","PH-BUK","PH-BUL","PH-CAG","PH-CAN","PH-CAS","PH-CAM","PH-CAP","PH-CAT","PH-CAV","PH-CEB","PH-NCO","PH-DAO","PH-DAV","PH-DAS","PH-EAS","PH-GUI","PH-IFU","PH-ILN","PH-ILS","PH-ILI","PH-ISA","PH-KAL","PH-LUN","PH-LAG","PH-LAN","PH-LAS","PH-LEY","PH-MAG","PH-MAD","PH-MAS","PH-00","PH-MSC","PH-MSR","PH-MOU","PH-NEC","PH-NER","PH-NSA","PH-NUE","PH-NUV","PH-MDC","PH-MDR","PH-PLW","PH-PAM","PH-PAN","PH-QUE","PH-QUI","PH-RIZ","PH-ROM","PH-WSA","PH-SAR","PH-SIG","PH-SOR","PH-SCO","PH-SLE","PH-SUK","PH-SLU","PH-SUN","PH-SUR","PH-TAR","PH-TAW","PH-ZMB","PH-ZAN","PH-ZAS","PH-ZSI","PT-20","PT-01","PT-02","PT-03","PT-04","PT-05","PT-06","PT-07","PT-08","PT-09","PT-10","PT-11","PT-30","PT-12","PT-13","PT-14","PT-15","PT-16","PT-17","PT-18","AB","AR","AG","BC","BH","BN","BT","BR","BV","B","BZ","CL","CS","CJ","CT","CV","DB","DJ","GL","GR","GJ","HR","HD","IL","IS","IF","MM","MH","MS","NT","OT","PH","SJ","SM","SB","SV","TR","TM","TL","VL","VS","VN","KR-26","KR-43","KR-44","KR-27","KR-30","KR-42","KR-29","KR-47","KR-41","KR-48","KR-28","KR-49","KR-45","KR-46","KR-50","KR-11","KR-31","C","VI","AB","A","AL","O","AV","BA","PM","B","BU","CC","CA","S","CS","CE","CR","CO","CU","GI","GR","GU","SS","H","HU","J","LO","GC","LE","L","LU","M","MA","ML","MU","NA","OR","P","PO","SA","TF","SG","SE","SO","T","TE","TO","V","VA","BI","ZA","Z","AZ","AJ","DU","FU","RK","SH","UQ","BFP","ENG","NIR","SCT","WLS","AR","CA","CL","CO","DU","FS","FD","LA","MA","MO","PA","RN","RV","RO","SA","SJ","SO","TA","TT"],"example":"US"},"city":{"type":"string","description":"City of the customer","example":"New York"},"zip":{"type":"string","description":"Zip code of the customer","example":"12345"},"phone":{"type":"string","description":"Phone number of the customer","example":"1234567890"},"email":{"type":"string","description":"Email of the customer","example":"abu@example.com"}},"required":["country"]}]},"amountAvailable":{"type":"string","description":"it will not calculate the order amount form backend if it is true","enum":["AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","GB","UA","AE","US","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"totalOrderAmount":{"type":"number","description":"The amount of the price. ( min: 0.01 )"},"weightAvailable":{"type":"boolean","description":"Flag to pass when the weight is already calculated and should not calculate again"},"totalOrderWeight":{"type":"number","description":"Estimated weight of the order calculated from the order creation side in kg(s)"},"source":{"description":"Source of the order","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Source of order","enum":["funnel","website","invoice","calendar","text2Pay","document_contracts","membership","mobile_app","communities","point_of_sale","manual","form","survey","payment_link","external"],"example":"website"},"subType":{"type":"string","description":"Source subtype of order","enum":["one_step_order_form","two_step_order_form","upsell","tap_to_pay","card_payment","store","contact_view","email_campaign","payments_dashboard","shopify","subscription_view","store_upsell","woocommerce","service","meeting","imported_csv","qr_code"],"example":"store"}},"required":["type"]}]},"products":{"description":"An array of price IDs and quantity","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"id of product"},"qty":{"type":"number","description":"No of quantities"}},"required":["id","qty"]}},"couponCode":{"type":"string","description":"Coupon code"}},"required":["altId","altType","country","totalOrderAmount","totalOrderWeight","source","products"],"description":"The JSON request body."}},"required":["Authorization","requestBody"]},
    method: "post",
    pathTemplate: "/store/shipping-zone/shipping-rates",
    executionParameters: [{"name":"Authorization","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearerAuth":[]}]
  }],
  ["list-shipping-rates", {
    name: "list-shipping-rates",
    description: `The "List Shipping Rate" API allows to retrieve a list of shipping rate.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the item that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."}},"required":["Authorization","shippingZoneId","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}/shipping-rate",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["create-shipping-rate", {
    name: "create-shipping-rate",
    description: `The "Create Shipping Rate" API allows adding a new shipping rate.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the item that needs to be returned"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping zone"},"description":{"type":"string","description":"Delivery description"},"currency":{"type":"string","description":"The currency of the amount of the rate / handling fee"},"amount":{"type":"number","description":"The amount of the shipping rate if it is normal rate (0 means free ). Fixed Handling fee if it is a carrier rate (it will add to the carrier rate)."},"conditionType":{"type":"string","description":"Type of condition to provide the conditional pricing","enum":["none","price","weight"]},"minCondition":{"type":"number","description":"Minimum condition for applying this price. set 0 or null if there is no minimum"},"maxCondition":{"type":"number","description":"Maximum condition for applying this price. set 0 or null if there is no maximum"},"isCarrierRate":{"type":"boolean","description":"is this a carrier rate"},"shippingCarrierId":{"type":"string","description":"Shipping carrier id"},"percentageOfRateFee":{"type":"number","description":"Percentage of rate fee if it is a carrier rate."},"shippingCarrierServices":{"description":"An array of items","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Name of the shipping carrier service"},"value":{"type":"string","description":"Value of the shipping carrier service"}},"required":["name","value"]}}},"required":["altId","altType","name","currency","amount","conditionType","minCondition","maxCondition","shippingCarrierId"],"description":"The JSON request body."}},"required":["Authorization","shippingZoneId","requestBody"]},
    method: "post",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}/shipping-rate",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-shipping-rates", {
    name: "get-shipping-rates",
    description: `The "List Shipping Rate" API allows to retrieve a paginated list of shipping rate.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the shipping zone"},"shippingRateId":{"type":"string","description":"ID of the shipping rate that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","shippingZoneId","shippingRateId","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}/shipping-rate/{shippingRateId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"shippingRateId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["update-shipping-rate", {
    name: "update-shipping-rate",
    description: `The "update Shipping Rate" API allows update a shipping rate to the system. `,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the shipping zone"},"shippingRateId":{"type":"string","description":"ID of the shipping rate that needs to be returned"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping zone"},"description":{"type":"string","description":"Delivery description"},"currency":{"type":"string","description":"The currency of the amount of the rate / handling fee"},"amount":{"type":"number","description":"The amount of the shipping rate if it is normal rate (0 means free ). Fixed Handling fee if it is a carrier rate (it will add to the carrier rate)."},"conditionType":{"type":"string","description":"Type of condition to provide the conditional pricing","enum":["none","price","weight"]},"minCondition":{"type":"number","description":"Minimum condition for applying this price. set 0 or null if there is no minimum"},"maxCondition":{"type":"number","description":"Maximum condition for applying this price. set 0 or null if there is no maximum"},"isCarrierRate":{"type":"boolean","description":"is this a carrier rate"},"shippingCarrierId":{"type":"string","description":"Shipping carrier id"},"percentageOfRateFee":{"type":"number","description":"Percentage of rate fee if it is a carrier rate."},"shippingCarrierServices":{"description":"An array of items","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Name of the shipping carrier service"},"value":{"type":"string","description":"Value of the shipping carrier service"}},"required":["name","value"]}}},"description":"The JSON request body."}},"required":["Authorization","shippingZoneId","shippingRateId","requestBody"]},
    method: "put",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}/shipping-rate/{shippingRateId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"shippingRateId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["delete-shipping-rate", {
    name: "delete-shipping-rate",
    description: `Delete specific shipping rate with Id :shippingRateId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingZoneId":{"type":"string","description":"ID of the shipping zone"},"shippingRateId":{"type":"string","description":"ID of the shipping rate that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","shippingZoneId","shippingRateId","altId","altType"]},
    method: "delete",
    pathTemplate: "/store/shipping-zone/{shippingZoneId}/shipping-rate/{shippingRateId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingZoneId","in":"path"},{"name":"shippingRateId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["list-shipping-carriers", {
    name: "list-shipping-carriers",
    description: `The "List Shipping Carrier" API allows to retrieve a list of shipping carrier.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-carrier",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["create-shipping-carrier", {
    name: "create-shipping-carrier",
    description: `The "Create Shipping Carrier" API allows adding a new shipping carrier.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping carrier"},"callbackUrl":{"type":"string","description":"The URL endpoint that GHL needs to retrieve shipping rates. This must be a public URL."},"services":{"description":"An array of available shipping carrier services","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Name of the shipping carrier service"},"value":{"type":"string","description":"Value of the shipping carrier service"}},"required":["name","value"]}},"allowsMultipleServiceSelection":{"type":"boolean","description":"The seller can choose multiple services while creating shipping rates if this is true."}},"required":["altId","altType","name","callbackUrl"],"description":"The JSON request body."}},"required":["Authorization","requestBody"]},
    method: "post",
    pathTemplate: "/store/shipping-carrier",
    executionParameters: [{"name":"Authorization","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-shipping-carriers", {
    name: "get-shipping-carriers",
    description: `The "List Shipping Carrier" API allows to retrieve a paginated list of shipping carrier.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingCarrierId":{"type":"string","description":"ID of the shipping carrier that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","shippingCarrierId","altId","altType"]},
    method: "get",
    pathTemplate: "/store/shipping-carrier/{shippingCarrierId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingCarrierId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["update-shipping-carrier", {
    name: "update-shipping-carrier",
    description: `The "update Shipping Carrier" API allows update a shipping carrier to the system. `,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingCarrierId":{"type":"string","description":"ID of the shipping carrier that needs to be returned"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"name":{"type":"string","description":"Name of the shipping carrier"},"callbackUrl":{"type":"string","description":"The URL endpoint that GHL needs to retrieve shipping rates. This must be a public URL."},"services":{"description":"An array of available shipping carrier services","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Name of the shipping carrier service"},"value":{"type":"string","description":"Value of the shipping carrier service"}},"required":["name","value"]}},"allowsMultipleServiceSelection":{"type":"boolean","description":"The seller can choose multiple services while creating shipping rates if this is true."}},"description":"The JSON request body."}},"required":["Authorization","shippingCarrierId","requestBody"]},
    method: "put",
    pathTemplate: "/store/shipping-carrier/{shippingCarrierId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingCarrierId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["delete-shipping-carrier", {
    name: "delete-shipping-carrier",
    description: `Delete specific shipping carrier with Id :shippingCarrierId`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"shippingCarrierId":{"type":"string","description":"ID of the shipping carrier that needs to be returned"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","shippingCarrierId","altId","altType"]},
    method: "delete",
    pathTemplate: "/store/shipping-carrier/{shippingCarrierId}",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"shippingCarrierId","in":"path"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-store-settings", {
    name: "get-store-settings",
    description: `Get store settings by altId and altType.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"enum":["location"],"type":"string"}},"required":["Authorization","altId","altType"]},
    method: "get",
    pathTemplate: "/store/store-setting",
    executionParameters: [{"name":"Authorization","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["create-store-setting", {
    name: "create-store-setting",
    description: `Create or update store settings by altId and altType.`,
    inputSchema: {"type":"object","properties":{"Authorization":{"type":"string","description":"Access Token"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"Location Id or Agency Id"},"altType":{"type":"string","enum":["location"]},"shippingOrigin":{"description":"Shipping origin address","allOf":[{"type":"object","properties":{"name":{"type":"string","description":"Name of the store / company","example":"ABC Store"},"country":{"type":"string","description":"Country code","enum":["US","CA","AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","KR","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"],"example":"US"},"state":{"type":"string","description":"State code","enum":["AL","AK","AS","AZ","AR","AA","AE","AP","CA","CO","CT","DE","DC","FM","FL","GA","GU","HI","ID","IL","IN","IA","KS","KY","LA","ME","MH","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","MP","OH","OK","OR","PW","PA","PR","RI","SC","SD","TN","TX","UT","VT","VI","VA","WA","WV","WI","WY","AB","BC","MB","NB","NL","NT","NS","NU","ON","PE","QC","SK","YT","BA","CT","CC","CH","CB","CN","ER","FO","JY","LP","LR","MZ","MN","NQ","RN","SA","SJ","SL","SC","SF","SE","TF","TU","ACT","NSW","NT","QLD","SA","TAS","VIC","WA","AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO","AI","AN","AP","AT","BI","CO","AR","LI","LL","LR","MA","ML","RM","TA","VS","NB","AMA","ANT","ARA","ATL","BOL","BOY","CAL","CAQ","CAS","CAU","CES","CHO","CUN","COR","GUA","GUV","HUI","LAG","MAG","MET","NAR","NSA","PUT","QUI","RIS","SAP","SAN","SUC","TOL","VAC","VAU","VID","CR-A","CR-C","CR-G","CR-H","CR-L","CR-P","CR-SJ","GT-16","GT-15","GT-04","GT-20","GT-02","GT-05","GT-01","GT-13","GT-18","GT-21","GT-22","GT-17","GT-09","GT-14","GT-11","GT-03","GT-12","GT-06","GT-07","GT-10","GT-08","GT-19","HK","KL","NT","AN","AP","AR","AS","BR","CH","CG","DN","DD","DL","GA","GJ","HR","HP","JK","JH","KA","KL","LA","LD","MP","MH","MN","ML","MZ","NL","OR","PY","PB","RJ","SK","TN","TS","TR","UP","UK","WB","CW","CN","CE","CO","DL","D","G","KY","KE","KK","LS","LM","LK","LD","LH","MO","MH","MN","OY","RN","SO","TA","WD","WH","WX","WW","AG","AL","AN","AO","AR","AP","AT","AV","BA","BT","BL","BN","BG","BI","BO","BZ","BS","BR","CA","CL","CB","CI","CE","CT","CZ","CH","CO","CS","CR","KR","CN","EN","FM","FE","FI","FG","FC","FR","GE","GO","GR","IM","IS","AQ","SP","LT","LE","LC","LI","LO","LU","MC","MN","MS","MT","VS","ME","MI","MO","MB","NA","NO","NU","OG","OT","OR","PD","PA","PR","PV","PG","PU","PE","PC","PI","PT","PN","PZ","PO","RG","RA","RC","RE","RI","RN","RM","RO","SA","SS","SV","SI","SR","SO","TA","TE","TR","TO","TP","TN","TV","TS","UD","VA","VE","VB","VC","VR","VV","VI","VT","JP-23","JP-05","JP-02","JP-12","JP-38","JP-18","JP-40","JP-07","JP-21","JP-10","JP-34","JP-01","JP-28","JP-08","JP-17","JP-03","JP-37","JP-46","JP-14","JP-39","JP-43","JP-26","JP-24","JP-04","JP-45","JP-20","JP-42","JP-29","JP-15","JP-44","JP-33","JP-47","JP-27","JP-41","JP-11","JP-25","JP-32","JP-22","JP-09","JP-36","JP-13","JP-31","JP-16","JP-30","JP-06","JP-35","JP-19","JHR","KDH","KTN","KUL","LBN","MLK","NSN","PHG","PNG","PRK","PLS","PJY","SBH","SWK","SGR","TRG","AGU","BCN","BCS","CAM","CHP","CHH","CMX","COA","COL","DUR","GUA","GRO","HID","JAL","MIC","MOR","MEX","NAY","NLE","OAX","PUE","QUE","ROO","SLP","SIN","SON","TAB","TAM","TLA","VER","YUC","ZAC","AUK","BOP","CAN","CIT","GIS","HKB","MWT","MBH","NSN","NTL","OTA","STL","TKI","TAS","WKO","WGN","WTC","JK","BA","GB","IS","KP","PB","SD","AMA","ANC","APU","ARE","AYA","CAJ","CAL","CUS","HUV","HUC","ICA","JUN","LAL","LAM","LIM","LOR","MDD","MOQ","PAS","PIU","PUN","SAM","TAC","TUM","UCA","PH-ABR","PH-AGN","PH-AGS","PH-AKL","PH-ALB","PH-ANT","PH-APA","PH-AUR","PH-BAS","PH-BAN","PH-BTN","PH-BTG","PH-BEN","PH-BIL","PH-BOH","PH-BUK","PH-BUL","PH-CAG","PH-CAN","PH-CAS","PH-CAM","PH-CAP","PH-CAT","PH-CAV","PH-CEB","PH-NCO","PH-DAO","PH-DAV","PH-DAS","PH-EAS","PH-GUI","PH-IFU","PH-ILN","PH-ILS","PH-ILI","PH-ISA","PH-KAL","PH-LUN","PH-LAG","PH-LAN","PH-LAS","PH-LEY","PH-MAG","PH-MAD","PH-MAS","PH-00","PH-MSC","PH-MSR","PH-MOU","PH-NEC","PH-NER","PH-NSA","PH-NUE","PH-NUV","PH-MDC","PH-MDR","PH-PLW","PH-PAM","PH-PAN","PH-QUE","PH-QUI","PH-RIZ","PH-ROM","PH-WSA","PH-SAR","PH-SIG","PH-SOR","PH-SCO","PH-SLE","PH-SUK","PH-SLU","PH-SUN","PH-SUR","PH-TAR","PH-TAW","PH-ZMB","PH-ZAN","PH-ZAS","PH-ZSI","PT-20","PT-01","PT-02","PT-03","PT-04","PT-05","PT-06","PT-07","PT-08","PT-09","PT-10","PT-11","PT-30","PT-12","PT-13","PT-14","PT-15","PT-16","PT-17","PT-18","AB","AR","AG","BC","BH","BN","BT","BR","BV","B","BZ","CL","CS","CJ","CT","CV","DB","DJ","GL","GR","GJ","HR","HD","IL","IS","IF","MM","MH","MS","NT","OT","PH","SJ","SM","SB","SV","TR","TM","TL","VL","VS","VN","KR-26","KR-43","KR-44","KR-27","KR-30","KR-42","KR-29","KR-47","KR-41","KR-48","KR-28","KR-49","KR-45","KR-46","KR-50","KR-11","KR-31","C","VI","AB","A","AL","O","AV","BA","PM","B","BU","CC","CA","S","CS","CE","CR","CO","CU","GI","GR","GU","SS","H","HU","J","LO","GC","LE","L","LU","M","MA","ML","MU","NA","OR","P","PO","SA","TF","SG","SE","SO","T","TE","TO","V","VA","BI","ZA","Z","AZ","AJ","DU","FU","RK","SH","UQ","BFP","ENG","NIR","SCT","WLS","AR","CA","CL","CO","DU","FS","FD","LA","MA","MO","PA","RN","RV","RO","SA","SJ","SO","TA","TT"],"example":"VA"},"city":{"type":"string","description":"City name","example":"Tokyo"},"street1":{"type":"string","description":"Street address line 1","example":"Street 1"},"street2":{"type":"string","description":"Street address line 2","example":"Street 2"},"zip":{"type":"string","description":"Zip code","example":"674561"},"phone":{"type":"string","description":"Business Phone Number","example":"+1-214-559-6993"},"email":{"type":"string","description":"Email","example":"john@deo.com"}},"required":["name","country","city","street1","zip"]}]},"storeOrderNotification":{"description":"Store order notification email","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Store order notification enabled","example":true},"subject":{"type":"string","description":"Store order email subject","example":"Your order is placed !"},"emailTemplateId":{"type":"string","description":"Email Template Id","example":"6788d542f0462ffd6bc29bb9"},"defaultEmailTemplateId":{"type":"string","description":"Default Email Template Id","example":"6788d542f0462ffd6bc29bb9"}},"required":["enabled","subject","emailTemplateId","defaultEmailTemplateId"]}]},"storeOrderFulfillmentNotification":{"description":"Store order fulfillment notification email","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Store order fulfillment notification enabled","example":true},"subject":{"type":"string","description":"Store order fulfillment email subject","example":"Order fulfilled"},"emailTemplateId":{"type":"string","description":"Email Template Id","example":"6788d542f0462ffd6bc29bb9"},"defaultEmailTemplateId":{"type":"string","description":"Default Email Template Id","example":"6788d542f0462ffd6bc29bb9"}},"required":["enabled","subject","emailTemplateId","defaultEmailTemplateId"]}]}},"required":["altId","altType","shippingOrigin"],"description":"The JSON request body."}},"required":["Authorization","requestBody"]},
    method: "post",
    pathTemplate: "/store/store-setting",
    executionParameters: [{"name":"Authorization","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":[]}]
  }],
  ["get-surveys-submissions", {
    name: "get-surveys-submissions",
    description: `Get Surveys Submissions`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"page":{"default":1,"type":"number","description":"Page No. By default it will be 1"},"limit":{"default":20,"type":"number","description":"Limit Per Page records count. will allow maximum up to 100 and default will be 20"},"surveyId":{"type":"string","description":"Filter submission by survey id"},"q":{"type":"string","description":"Filter by contactId, name, email or phone no."},"startAt":{"type":"string","description":"Get submission by starting of this date. By default it will be same date of last month(YYYY-MM-DD)."},"endAt":{"type":"string","description":"Get submission by ending of this date. By default it will be current date(YYYY-MM-DD)."}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/surveys/submissions",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"page","in":"query"},{"name":"limit","in":"query"},{"name":"surveyId","in":"query"},{"name":"q","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["surveys.readonly"]}]
  }],
  ["search-users", {
    name: "search-users",
    description: `Search Users`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"companyId":{"type":"string","description":"Company ID in which the search needs to be performed"},"query":{"type":"string","description":"The search term for the user is matched based on the user full name, email or phone"},"skip":{"default":"0","type":"string","description":"No of results to be skipped before returning the result"},"limit":{"default":"25","type":"string","description":"No of results to be limited before returning the result"},"locationId":{"type":"string","description":"Location ID in which the search needs to be performed"},"type":{"type":"string","description":"Type of the users to be filtered in the search"},"role":{"type":"string","description":"Role of the users to be filtered in the search"},"ids":{"type":"string","description":"List of User IDs to be filtered in the search"},"sort":{"type":"string","description":"The field on which sort is applied in which the results need to be sorted. Default is based on the first and last name"},"sortDirection":{"type":"string","description":"The direction in which the results need to be sorted"},"enabled2waySync":{"type":"boolean"}},"required":["Version","companyId"]},
    method: "get",
    pathTemplate: "/users/search",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"query"},{"name":"query","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"locationId","in":"query"},{"name":"type","in":"query"},{"name":"role","in":"query"},{"name":"ids","in":"query"},{"name":"sort","in":"query"},{"name":"sortDirection","in":"query"},{"name":"enabled2waySync","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["users.readonly"]},{"Location-Access":["users.readonly"]}]
  }],
  ["filter-users-by-email", {
    name: "filter-users-by-email",
    description: `Filter users by company ID, deleted status, and email array`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"companyId":{"type":"string","description":"Company ID to filter users"},"emails":{"description":"Array of email addresses to filter users","type":"array","items":{"type":"string"}},"deleted":{"type":"boolean","description":"Filter deleted users","default":false},"skip":{"type":"string","default":"0","description":"No of results to be skipped before returning the result"},"limit":{"type":"string","default":"25","description":"No of results to be limited before returning the result"},"projection":{"type":"string","description":"Projection fields to return. Use \"all\" for all fields, or specify comma-separated field names. Default returns only id and email"}},"required":["companyId","emails"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/users/search/filter-by-email",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["users.readonly"]},{"Location-Access":["users.readonly"]}]
  }],
  ["get-user", {
    name: "get-user",
    description: `Get User`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"userId":{"type":"string","description":"User Id"}},"required":["Version","userId"]},
    method: "get",
    pathTemplate: "/users/{userId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"userId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["users.readonly"]},{"Location-Access":["users.readonly"]}]
  }],
  ["update-user", {
    name: "update-user",
    description: `Update User`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"userId":{"type":"string"},"requestBody":{"type":"object","properties":{"firstName":{"type":"string"},"lastName":{"type":"string"},"email":{"type":"string","description":"Email update is no longer supported due to security reasons."},"emailChangeOTP":{"type":"string","description":"OTP to change the email ID of the user"},"password":{"type":"string"},"phone":{"type":"string"},"type":{"type":"string"},"role":{"type":"string"},"companyId":{"type":"string","description":"Company/Agency Id. Required for Agency Level access"},"locationIds":{"type":"array","items":{"type":"string"}},"permissions":{"type":"object","properties":{"campaignsEnabled":{"type":"boolean","default":true},"campaignsReadOnly":{"type":"boolean","default":false},"contactsEnabled":{"type":"boolean","default":true},"workflowsEnabled":{"type":"boolean","default":true},"workflowsReadOnly":{"type":"boolean","default":false},"triggersEnabled":{"type":"boolean","default":true},"funnelsEnabled":{"type":"boolean","default":true},"websitesEnabled":{"type":"boolean","default":false},"opportunitiesEnabled":{"type":"boolean","default":true},"dashboardStatsEnabled":{"type":"boolean","default":true},"bulkRequestsEnabled":{"type":"boolean","default":true},"appointmentsEnabled":{"type":"boolean","default":true},"reviewsEnabled":{"type":"boolean","default":true},"onlineListingsEnabled":{"type":"boolean","default":true},"phoneCallEnabled":{"type":"boolean","default":true},"conversationsEnabled":{"type":"boolean","default":true},"assignedDataOnly":{"type":"boolean","default":false},"adwordsReportingEnabled":{"type":"boolean","default":false},"membershipEnabled":{"type":"boolean","default":false},"facebookAdsReportingEnabled":{"type":"boolean","default":false},"attributionsReportingEnabled":{"type":"boolean","default":false},"settingsEnabled":{"type":"boolean","default":true},"tagsEnabled":{"type":"boolean","default":true},"leadValueEnabled":{"type":"boolean","default":true},"marketingEnabled":{"type":"boolean","default":true},"agentReportingEnabled":{"type":"boolean","default":true},"botService":{"type":"boolean","default":false},"socialPlanner":{"type":"boolean","default":true},"bloggingEnabled":{"type":"boolean","default":true},"invoiceEnabled":{"type":"boolean","default":true},"affiliateManagerEnabled":{"type":"boolean","default":true},"contentAiEnabled":{"type":"boolean","default":true},"refundsEnabled":{"type":"boolean","default":true},"recordPaymentEnabled":{"type":"boolean","default":true},"cancelSubscriptionEnabled":{"type":"boolean","default":true},"paymentsEnabled":{"type":"boolean","default":true},"communitiesEnabled":{"type":"boolean","default":true},"exportPaymentsEnabled":{"type":"boolean","default":true}}},"scopes":{"type":"array","description":"Scopes allowed for users. Only scopes that have been passed will be enabled. If passed empty all the scopes will be get disabled","items":{"type":"string","enum":["campaigns.readonly","campaigns.write","calendars.readonly","calendars/events.write","calendars/groups.write","calendars.write","contacts.write","contacts/bulkActions.write","workflows.readonly","workflows.write","triggers.write","funnels.write","forms.write","surveys.write","quizzes.write","websites.write","medias.write","medias.readonly","opportunities.write","opportunities/leadValue.readonly","opportunities/bulkActions.write","reporting/phone.readonly","reporting/adwords.readonly","reporting/facebookAds.readonly","reporting/attributions.readonly","prospecting/auditReport.write","reporting/reports.readonly","reporting/agent.readonly","reporting/reports.write","payments.write","payments/refunds.write","payments/records.write","payments/exports.write","payments/subscriptionsCancel.write","invoices.write","invoices.readonly","invoices/schedule.readonly","invoices/schedule.write","invoices/template.readonly","invoices/template.write","reputation/review.write","reputation/listing.write","reputation/reviewsAIAgents.write","conversations.write","conversations.readonly","conversations/message.readonly","conversations/message.write","contentAI.write","dashboard/stats.readonly","locations/tags.write","locations/tags.readonly","marketing.write","eliza.write","settings.write","socialplanner/post.write","socialplanner/account.readonly","socialplanner/account.write","socialplanner/category.readonly","socialplanner/category.write","socialplanner/csv.readonly","socialplanner/csv.write","socialplanner/group.write","socialplanner/hashtag.readonly","socialplanner/hashtag.write","socialplanner/oauth.readonly","socialplanner/oauth.write","socialplanner/post.readonly","socialplanner/recurring.readonly","socialplanner/recurring.write","socialplanner/review.readonly","socialplanner/review.write","socialplanner/rss.readonly","socialplanner/rss.write","socialplanner/search.readonly","socialplanner/setting.readonly","socialplanner/setting.write","socialplanner/stat.readonly","socialplanner/tag.readonly","socialplanner/tag.write","socialplanner/filters.readonly","socialplanner/medias.readonly","socialplanner/medias.write","socialplanner/watermarks.readonly","socialplanner/watermarks.write","socialplanner/metatag.readonly","socialplanner/facebook.readonly","socialplanner/linkedin.readonly","socialplanner/twitter.readonly","socialplanner/notification.readonly","socialplanner/notification.write","socialplanner/snapshot.readonly","socialplanner/snapshot.write","marketing/affiliate.write","blogs.write","membership.write","communities.write","gokollab.write","certificates.write","certificates.readonly","adPublishing.write","adPublishing.readonly","prospecting.write","prospecting.readonly","prospecting/reports.readonly","private-integration-location.readonly","private-integration-location.write","private-integration-company.readonly","private-integration-company.write","native-integrations.readonly","native-integrations.write","wordpress.write","wordpress.read","custom-menu-link.write","qrcodes.write","users/team-management.write","users/team-management.readonly","loginas.write","snapshots/api.readonly","snapshots/api.create","snapshots/api.edit","snapshots/api.push","snapshots/api.refresh","snapshots/api.share","snapshots/api.delete","internaltools.location-transfer.write","internaltools.location-transfer.readonly","affiliateportal.write","affiliateportal.readonly","companies.write","internaltools.billing.write","internaltools.billing.readonly","internaltools.billing-common.readonly","internaltools.billing-common.write","voice-ai-agents.write","voice-ai-agent-goals.readonly","voice-ai-agent-goals.write","voice-ai-dashboard.readonly","agency/launchpad.write","agency/launchpad.readonly","launchpad.write","launchpad.readonly","text-ai-agents.write","text-ai-agent-goals.readonly","text-ai-agent-goals.write","text-ai-agent-training.write"]}},"scopesAssignedToOnly":{"type":"array","description":"Assigned Scopes allowed for users. Only scopes that have been passed will be enabled. If passed empty all the assigned scopes will be get disabled","items":{"type":"string","enum":["campaigns.readonly","campaigns.write","calendars.readonly","calendars/events.write","calendars/groups.write","calendars.write","contacts.write","contacts/bulkActions.write","workflows.readonly","workflows.write","triggers.write","funnels.write","forms.write","surveys.write","quizzes.write","websites.write","medias.write","medias.readonly","opportunities.write","opportunities/leadValue.readonly","opportunities/bulkActions.write","reporting/phone.readonly","reporting/adwords.readonly","reporting/facebookAds.readonly","reporting/attributions.readonly","prospecting/auditReport.write","reporting/reports.readonly","reporting/agent.readonly","reporting/reports.write","payments.write","payments/refunds.write","payments/records.write","payments/exports.write","payments/subscriptionsCancel.write","invoices.write","invoices.readonly","invoices/schedule.readonly","invoices/schedule.write","invoices/template.readonly","invoices/template.write","reputation/review.write","reputation/listing.write","reputation/reviewsAIAgents.write","conversations.write","conversations.readonly","conversations/message.readonly","conversations/message.write","contentAI.write","dashboard/stats.readonly","locations/tags.write","locations/tags.readonly","marketing.write","eliza.write","settings.write","socialplanner/post.write","socialplanner/account.readonly","socialplanner/account.write","socialplanner/category.readonly","socialplanner/category.write","socialplanner/csv.readonly","socialplanner/csv.write","socialplanner/group.write","socialplanner/hashtag.readonly","socialplanner/hashtag.write","socialplanner/oauth.readonly","socialplanner/oauth.write","socialplanner/post.readonly","socialplanner/recurring.readonly","socialplanner/recurring.write","socialplanner/review.readonly","socialplanner/review.write","socialplanner/rss.readonly","socialplanner/rss.write","socialplanner/search.readonly","socialplanner/setting.readonly","socialplanner/setting.write","socialplanner/stat.readonly","socialplanner/tag.readonly","socialplanner/tag.write","socialplanner/filters.readonly","socialplanner/medias.readonly","socialplanner/medias.write","socialplanner/watermarks.readonly","socialplanner/watermarks.write","socialplanner/metatag.readonly","socialplanner/facebook.readonly","socialplanner/linkedin.readonly","socialplanner/twitter.readonly","socialplanner/notification.readonly","socialplanner/notification.write","socialplanner/snapshot.readonly","socialplanner/snapshot.write","marketing/affiliate.write","blogs.write","membership.write","communities.write","gokollab.write","certificates.write","certificates.readonly","adPublishing.write","adPublishing.readonly","prospecting.write","prospecting.readonly","prospecting/reports.readonly","private-integration-location.readonly","private-integration-location.write","private-integration-company.readonly","private-integration-company.write","native-integrations.readonly","native-integrations.write","wordpress.write","wordpress.read","custom-menu-link.write","qrcodes.write","users/team-management.write","users/team-management.readonly","loginas.write","snapshots/api.readonly","snapshots/api.create","snapshots/api.edit","snapshots/api.push","snapshots/api.refresh","snapshots/api.share","snapshots/api.delete","internaltools.location-transfer.write","internaltools.location-transfer.readonly","affiliateportal.write","affiliateportal.readonly","companies.write","internaltools.billing.write","internaltools.billing.readonly","internaltools.billing-common.readonly","internaltools.billing-common.write","voice-ai-agents.write","voice-ai-agent-goals.readonly","voice-ai-agent-goals.write","voice-ai-dashboard.readonly","agency/launchpad.write","agency/launchpad.readonly","launchpad.write","launchpad.readonly","text-ai-agents.write","text-ai-agent-goals.readonly","text-ai-agent-goals.write","text-ai-agent-training.write"]}},"profilePhoto":{"type":"string"}},"description":"The JSON request body."}},"required":["Version","userId","requestBody"]},
    method: "put",
    pathTemplate: "/users/{userId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"userId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["users.write"]},{"Location-Access":["users.write"]}]
  }],
  ["delete-user", {
    name: "delete-user",
    description: `Delete User`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"userId":{"type":"string"}},"required":["Version","userId"]},
    method: "delete",
    pathTemplate: "/users/{userId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"userId","in":"path"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["users.write"]},{"Location-Access":["users.write"]}]
  }],
  ["get-agents", {
    name: "get-agents",
    description: `Retrieve a paginated list of agents for given location.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"page":{"minimum":1,"maximum":5000,"default":1,"type":"number","description":"Page number starting from 1"},"pageSize":{"minimum":1,"maximum":50,"default":10,"type":"number","description":"Number of items per page"},"locationId":{"type":"string","description":"Location ID"},"query":{"type":"string","description":"Query"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/voice-ai/agents",
    executionParameters: [{"name":"Version","in":"header"},{"name":"page","in":"query"},{"name":"pageSize","in":"query"},{"name":"locationId","in":"query"},{"name":"query","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-agents.readonly"]}]
  }],
  ["create-agent", {
    name: "create-agent",
    description: `Create a new voice AI agent configuration and settings`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Unique identifier for the location where this agent will operate"},"agentName":{"type":"string","description":"Display name for the voice AI agent, between 1-40 characters. Default: \"My Agent {random 3 digit number}\"","maxLength":40,"minLength":1},"businessName":{"type":"string","description":"Name of the business this agent represents. Default: Uses location name","minLength":1},"welcomeMessage":{"type":"string","description":"Initial greeting spoken when the agent answers calls. Default: Auto generated","maxLength":190,"minLength":1},"agentPrompt":{"type":"string","description":"Custom instructions defining the agent's behavior and personality. Default: Basic prompt generated automatically"},"voiceId":{"type":"string","description":"Identifier for the speech synthesis voice from available voice options. Default: Auto generated"},"language":{"type":"string","description":"Language code for the agent's speech and understanding. Default: \"en-US\"","enum":["en-US","pt-BR","es","fr","de","it","nl-NL","multi"]},"patienceLevel":{"type":"string","description":"Tolerance level for caller response delays. Default: \"high\"","enum":["low","medium","high"]},"maxCallDuration":{"type":"number","description":"Maximum call duration in seconds, between 180-900 (3-15 minutes). Default: 300 seconds (5 minutes)","minimum":180,"maximum":900},"sendUserIdleReminders":{"type":"boolean","description":"Enables automatic reminders when callers are silent. Default: true"},"reminderAfterIdleTimeSeconds":{"type":"number","description":"Seconds to wait before sending idle reminders, between 1-20. Default: 8 seconds","minimum":1,"maximum":20},"inboundNumber":{"type":"string","description":"Phone number for receiving inbound calls to this agent. Default: null"},"numberPoolId":{"type":"string","description":"Identifier for the number pool managing phone number allocation. Default: null"},"callEndWorkflowIds":{"description":"Array of workflow IDs to trigger automatically when calls end. Default: []","maxItems":10,"type":"array","items":{"type":"string"}},"sendPostCallNotificationTo":{"description":"Configuration for post-call email notifications to various recipients. Default: []","allOf":[{"type":"object","properties":{"admins":{"type":"boolean","description":"Enables post-call notifications to all admin users in the location. Default: true","example":true},"allUsers":{"type":"boolean","description":"Enables post-call notifications to all users in the location. Default: false","example":false},"contactAssignedUser":{"type":"boolean","description":"Enables post-call notifications to the user assigned to the contact. Default: false","example":false},"specificUsers":{"description":"Array of specific user IDs to receive post-call notifications. Default: []","example":["user_507f1f77bcf86cd799439011"],"type":"array","items":{"type":"string"}},"customEmails":{"description":"Array of custom email addresses to receive post-call notifications. Default: []","example":["manager@company.com"],"type":"array","items":{"type":"string"}}},"required":["admins","allUsers","contactAssignedUser","specificUsers","customEmails"]}]},"agentWorkingHours":{"description":"Time intervals defining when the agent accepts calls, organized by day of week. Default: [] (available 24/7)","type":"array","items":{"type":"object","properties":{"dayOfTheWeek":{"type":"number","enum":[1,2,3,4,5,6,7],"description":"Day of the week for this working hours configuration (Monday=1 to Sunday=7)"},"intervals":{"description":"Array of time intervals when the agent is available on this day","type":"array","items":{"type":"object","properties":{"startHour":{"type":"number","description":"Starting hour of the working interval in 24-hour format (0-23)","minimum":0,"maximum":23},"endHour":{"type":"number","description":"Ending hour of the working interval in 24-hour format (0-23)","minimum":0,"maximum":23},"startMinute":{"type":"number","description":"Starting minute of the working interval (0-59)","minimum":0,"maximum":59},"endMinute":{"type":"number","description":"Ending minute of the working interval (0-59)","minimum":0,"maximum":59}},"required":["startHour","endHour","startMinute","endMinute"]}}},"required":["dayOfTheWeek","intervals"]}},"timezone":{"type":"string","description":"IANA timezone identifier affecting working hours and scheduling. Default: Location timezone","pattern":"^[A-Za-z_]+/[A-Za-z_]+$"},"isAgentAsBackupDisabled":{"type":"boolean","description":"Prevents this agent from being used as a fallback option. Default: false (Available as backup agent)"},"translation":{"description":"Language translation settings including enablement flag and target language code. Rules: (1) translation.enabled can only be true if the agent's language is not en-US; (2) when enabled, translation.language must be either the agent's language or en-US; (3) when enabled, translation.language is required.","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Enables language translation for agent conversations. Default: false","example":false},"language":{"type":"string","description":"Target language code for translation (e.g., \"es\" for Spanish, \"fr\" for French).","example":"es"}},"required":["enabled"]}]}},"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/voice-ai/agents",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["voice-ai-agents.write"]}]
  }],
  ["get-agent", {
    name: "get-agent",
    description: `Retrieve detailed configuration and settings for a specific voice AI agent`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"agentId":{"type":"string","description":"Unique agent identifier"},"locationId":{"type":"string","description":"Location ID"}},"required":["Version","agentId","locationId"]},
    method: "get",
    pathTemplate: "/voice-ai/agents/{agentId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"agentId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-agents.readonly"]}]
  }],
  ["delete-agent", {
    name: "delete-agent",
    description: `Delete a voice AI agent and all its configurations`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"agentId":{"type":"string","description":"Unique agent identifier"},"locationId":{"type":"string","description":"Location ID"}},"required":["Version","agentId","locationId"]},
    method: "delete",
    pathTemplate: "/voice-ai/agents/{agentId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"agentId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-agents.write"]}]
  }],
  ["patch-agent", {
    name: "patch-agent",
    description: `Partially update an existing voice AI agent`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"agentId":{"type":"string","description":"Unique agent identifier"},"locationId":{"type":"string","description":"Location ID"},"requestBody":{"type":"object","properties":{"agentName":{"type":"string","description":"Display name for the voice AI agent, between 1-40 characters. Default: \"My Agent {random 3 digit number}\"","maxLength":40,"minLength":1},"businessName":{"type":"string","description":"Name of the business this agent represents. Default: Uses location name","minLength":1},"welcomeMessage":{"type":"string","description":"Initial greeting spoken when the agent answers calls. Default: Auto generated","maxLength":190,"minLength":1},"agentPrompt":{"type":"string","description":"Custom instructions defining the agent's behavior and personality. Default: Basic prompt generated automatically"},"voiceId":{"type":"string","description":"Identifier for the speech synthesis voice from available voice options. Default: Auto generated"},"language":{"type":"string","description":"Language code for the agent's speech and understanding. Default: \"en-US\"","enum":["en-US","pt-BR","es","fr","de","it","nl-NL","multi"]},"patienceLevel":{"type":"string","description":"Tolerance level for caller response delays. Default: \"high\"","enum":["low","medium","high"]},"maxCallDuration":{"type":"number","description":"Maximum call duration in seconds, between 180-900 (3-15 minutes). Default: 300 seconds (5 minutes)","minimum":180,"maximum":900},"sendUserIdleReminders":{"type":"boolean","description":"Enables automatic reminders when callers are silent. Default: true"},"reminderAfterIdleTimeSeconds":{"type":"number","description":"Seconds to wait before sending idle reminders, between 1-20. Default: 8 seconds","minimum":1,"maximum":20},"inboundNumber":{"type":"string","description":"Phone number for receiving inbound calls to this agent. Default: null"},"numberPoolId":{"type":"string","description":"Identifier for the number pool managing phone number allocation. Default: null"},"callEndWorkflowIds":{"description":"Array of workflow IDs to trigger automatically when calls end. Default: []","maxItems":10,"type":"array","items":{"type":"string"}},"sendPostCallNotificationTo":{"description":"Configuration for post-call email notifications to various recipients. Default: []","allOf":[{"type":"object","properties":{"admins":{"type":"boolean","description":"Enables post-call notifications to all admin users in the location. Default: true","example":true},"allUsers":{"type":"boolean","description":"Enables post-call notifications to all users in the location. Default: false","example":false},"contactAssignedUser":{"type":"boolean","description":"Enables post-call notifications to the user assigned to the contact. Default: false","example":false},"specificUsers":{"description":"Array of specific user IDs to receive post-call notifications. Default: []","example":["user_507f1f77bcf86cd799439011"],"type":"array","items":{"type":"string"}},"customEmails":{"description":"Array of custom email addresses to receive post-call notifications. Default: []","example":["manager@company.com"],"type":"array","items":{"type":"string"}}},"required":["admins","allUsers","contactAssignedUser","specificUsers","customEmails"]}]},"agentWorkingHours":{"description":"Time intervals defining when the agent accepts calls, organized by day of week. Default: [] (available 24/7)","type":"array","items":{"type":"object","properties":{"dayOfTheWeek":{"type":"number","enum":[1,2,3,4,5,6,7],"description":"Day of the week for this working hours configuration (Monday=1 to Sunday=7)"},"intervals":{"description":"Array of time intervals when the agent is available on this day","type":"array","items":{"type":"object","properties":{"startHour":{"type":"number","description":"Starting hour of the working interval in 24-hour format (0-23)","minimum":0,"maximum":23},"endHour":{"type":"number","description":"Ending hour of the working interval in 24-hour format (0-23)","minimum":0,"maximum":23},"startMinute":{"type":"number","description":"Starting minute of the working interval (0-59)","minimum":0,"maximum":59},"endMinute":{"type":"number","description":"Ending minute of the working interval (0-59)","minimum":0,"maximum":59}},"required":["startHour","endHour","startMinute","endMinute"]}}},"required":["dayOfTheWeek","intervals"]}},"timezone":{"type":"string","description":"IANA timezone identifier affecting working hours and scheduling. Default: Location timezone","pattern":"^[A-Za-z_]+/[A-Za-z_]+$"},"isAgentAsBackupDisabled":{"type":"boolean","description":"Prevents this agent from being used as a fallback option. Default: false (Available as backup agent)"},"translation":{"description":"Language translation settings including enablement flag and target language code. Rules: (1) translation.enabled can only be true if the agent's language is not en-US; (2) when enabled, translation.language must be either the agent's language or en-US; (3) when enabled, translation.language is required.","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Enables language translation for agent conversations. Default: false","example":false},"language":{"type":"string","description":"Target language code for translation (e.g., \"es\" for Spanish, \"fr\" for French).","example":"es"}},"required":["enabled"]}]}},"description":"The JSON request body."}},"required":["Version","agentId","locationId","requestBody"]},
    method: "patch",
    pathTemplate: "/voice-ai/agents/{agentId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"agentId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["voice-ai-agents.write"]}]
  }],
  ["get-call-logs", {
    name: "get-call-logs",
    description: `Returns call logs for Voice AI agents scoped to a location. Supports filtering by agent, contact, call type, action types, and date range (interpreted in the provided IANA timezone). Also supports sorting and 1-based pagination.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location identifier. Filters results to this location."},"agentId":{"type":"string","description":"Agent identifier. When provided, returns logs for this agent only."},"contactId":{"type":"string","description":"Contact IDs (comma-separated) to filter by."},"callType":{"enum":["LIVE","TRIAL"],"type":"string","description":"Call type filter."},"startDate":{"type":"number","description":"Start date filter (Unix timestamp). Must be less than endDate. Both startDate and endDate must be provided together."},"endDate":{"type":"number","description":"End date filter (Unix timestamp). Must be greater than startDate. Both startDate and endDate must be provided together."},"actionType":{"enum":["CALL_TRANSFER","DATA_EXTRACTION","IN_CALL_DATA_EXTRACTION","WORKFLOW_TRIGGER","SMS","APPOINTMENT_BOOKING","CUSTOM_ACTION","KNOWLEDGE_BASE"],"type":"string","description":"Action type filter for call logs (comma-separated ACTION_TYPE values)"},"sortBy":{"enum":["duration","createdAt"],"type":"string","description":"Field to sort by. Defaults to newest if omitted."},"sort":{"enum":["ascend","descend"],"type":"string","description":"Sort direction. Applies only when sortBy is provided."},"page":{"default":1,"type":"number","description":"Page number (1-based)."},"pageSize":{"default":10,"type":"number","description":"Page size (max 50)."}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/voice-ai/dashboard/call-logs",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"agentId","in":"query"},{"name":"contactId","in":"query"},{"name":"callType","in":"query"},{"name":"startDate","in":"query"},{"name":"endDate","in":"query"},{"name":"actionType","in":"query"},{"name":"sortBy","in":"query"},{"name":"sort","in":"query"},{"name":"page","in":"query"},{"name":"pageSize","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-dashboard.readonly"]}]
  }],
  ["getCallLog", {
    name: "getCallLog",
    description: `Returns a call log by callId.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"callId":{"type":"string","description":"Call ID"},"locationId":{"type":"string","description":"Location ID"}},"required":["Version","callId","locationId"]},
    method: "get",
    pathTemplate: "/voice-ai/dashboard/call-logs/{callId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"callId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-dashboard.readonly"]}]
  }],
  ["create-action", {
    name: "create-action",
    description: `Create a new action for a voice AI agent. Actions define specific behaviors and capabilities for the agent during calls.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"agentId":{"type":"string","description":"Agent ID to attach the action to"},"locationId":{"type":"string","description":"Location ID"},"actionType":{"type":"string","description":"Type of action","enum":["CALL_TRANSFER","DATA_EXTRACTION","IN_CALL_DATA_EXTRACTION","WORKFLOW_TRIGGER","SMS","APPOINTMENT_BOOKING","CUSTOM_ACTION"]},"name":{"type":"string","description":"Human-readable name for this action"},"actionParameters":{"description":"Action parameters - structure varies by actionType","type":"object","additionalProperties":true}},"required":["agentId","locationId","actionType","name","actionParameters"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/voice-ai/actions",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["voice-ai-agent-goals.write"]}]
  }],
  ["get-action", {
    name: "get-action",
    description: `Retrieve details of a specific action by its ID. Returns the action configuration including actionParameters.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"actionId":{"type":"string","description":"Unique identifier for the action"},"locationId":{"type":"string","description":"Location ID"}},"required":["Version","actionId","locationId"]},
    method: "get",
    pathTemplate: "/voice-ai/actions/{actionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"actionId","in":"path"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-agent-goals.readonly"]}]
  }],
  ["update-action", {
    name: "update-action",
    description: `Update an existing action for a voice AI agent. Modifies the behavior and configuration of an agent action.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"actionId":{"type":"string","description":"Unique identifier for the action"},"requestBody":{"type":"object","properties":{"agentId":{"type":"string","description":"Agent ID to attach the action to"},"locationId":{"type":"string","description":"Location ID"},"actionType":{"type":"string","description":"Type of action","enum":["CALL_TRANSFER","DATA_EXTRACTION","IN_CALL_DATA_EXTRACTION","WORKFLOW_TRIGGER","SMS","APPOINTMENT_BOOKING","CUSTOM_ACTION"]},"name":{"type":"string","description":"Human-readable name for this action"},"actionParameters":{"description":"Action parameters - structure varies by actionType","type":"object","additionalProperties":true}},"required":["agentId","locationId","actionType","name","actionParameters"],"description":"The JSON request body."}},"required":["Version","actionId","requestBody"]},
    method: "put",
    pathTemplate: "/voice-ai/actions/{actionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"actionId","in":"path"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["voice-ai-agent-goals.write"]}]
  }],
  ["delete-action", {
    name: "delete-action",
    description: `Delete an existing action from a voice AI agent. This permanently removes the action and its configuration.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"actionId":{"type":"string","description":"Unique identifier for the action"},"locationId":{"type":"string","description":"Location ID"},"agentId":{"type":"string","description":"Agent ID the action is attached to"}},"required":["Version","actionId","locationId","agentId"]},
    method: "delete",
    pathTemplate: "/voice-ai/actions/{actionId}",
    executionParameters: [{"name":"Version","in":"header"},{"name":"actionId","in":"path"},{"name":"locationId","in":"query"},{"name":"agentId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["voice-ai-agent-goals.write"]}]
  }],
  ["find-associations", {
    name: "find-associations",
    description: `Get all Associations`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"skip":{"type":"number"},"limit":{"type":"number"}},"required":["Version","locationId","skip","limit"]},
    method: "get",
    pathTemplate: "/associations",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["associations.readonly"]}]
  }],
  ["create-association-2", {
    name: "create-association-2",
    description: `Allow you to create contact - contact , contact - custom objects associations, will add more in the future.Documentation Link - https://doc.clickup.com/8631005/d/h/87cpx-293776/cd0f4122abc04d3`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"key":{"type":"string","description":"Association's Unique key"},"firstObjectLabel":{"type":"object","description":"First Objects Association Label (custom_objects.children)"},"firstObjectKey":{"type":"object","description":"First Objects Key"},"secondObjectLabel":{"type":"object","description":"Second Object Association Label (contact)"},"secondObjectKey":{"type":"object","description":"Second Objects Key"}},"required":["locationId","key","firstObjectLabel","firstObjectKey","secondObjectLabel","secondObjectKey"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/associations",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["associations.write"]}]
  }],
  ["get-businesses-by-location", {
    name: "get-businesses-by-location",
    description: `Get Businesses by Location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/businesses",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["businesses.readonly"]}]
  }],
  ["create-business", {
    name: "create-business",
    description: `Create Business`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"locationId":{"type":"string"},"phone":{"type":"string"},"email":{"type":"string"},"website":{"type":"string"},"address":{"type":"string"},"city":{"type":"string"},"postalCode":{"type":"string"},"state":{"type":"string"},"country":{"type":"string"},"description":{"type":"string"}},"required":["name","locationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/businesses",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["businesses.write"]}]
  }],
  ["get-calendars", {
    name: "get-calendars",
    description: `Get all calendars in a location.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"groupId":{"type":"string","description":"Group Id"},"showDrafted":{"default":true,"type":"boolean","description":"Show drafted"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/calendars",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"groupId","in":"query"},{"name":"showDrafted","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["calendars.readonly"]}]
  }],
  ["create-calendar", {
    name: "create-calendar",
    description: `Create calendar in a location.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"isActive":{"type":"boolean","description":"Should the created calendar be active or draft","default":true},"notifications":{"description":"🚨 Deprecated! Please use 'Calendar Notifications APIs' instead.","type":"array","items":{"type":"object","properties":{"type":{"type":"string","description":"Calendar Notification","enum":["email"],"default":"email"},"shouldSendToContact":{"type":"boolean"},"shouldSendToGuest":{"type":"boolean"},"shouldSendToUser":{"type":"boolean"},"shouldSendToSelectedUsers":{"type":"boolean"},"selectedUsers":{"type":"string","description":"Comma separated emails"}},"required":["shouldSendToContact","shouldSendToGuest","shouldSendToUser","shouldSendToSelectedUsers","selectedUsers"]}},"locationId":{"type":"string"},"groupId":{"type":"string","description":"Group Id"},"teamMembers":{"description":"Team members are required for calendars of type: Round Robin, Collective, Class, Service. Personal calendar must have exactly one team member.","type":"array","items":{"type":"object","properties":{"userId":{"type":"string"},"priority":{"type":"number","default":0.5,"enum":[0,0.5,1]},"meetingLocationType":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.kind` instead.","default":"custom","enum":["custom","zoom","gmeet","phone","address","teams","booker"]},"meetingLocation":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.location` instead."},"isPrimary":{"type":"boolean","description":"Marks a user as primary. This property is required in case of collective booking calendars. Only one user can be primary."},"locationConfigurations":{"description":"Meeting location configuration for event calendar.\n- *Multiple locations are allowed only when one team member is selected.*\n- *For **Class booking** and **Collective** calendars, only one location configuration is allowed for each team member.*","type":"array","items":{"type":"object","properties":{"kind":{"type":"string","description":"Type of meeting location. zoom_conference/google_conference/ms_teams_conference is not supported in event calendar type","enum":["custom","zoom_conference","google_conference","inbound_call","outbound_call","physical","booker","ms_teams_conference"]},"location":{"type":"string","description":"Address for meeting location. Not applicable on \"zoom_conference\", \"google_conference\" and \"ms_teams_conference\" kind"}},"required":["kind"]}}},"required":["userId"]}},"eventType":{"type":"string","enum":["RoundRobin_OptimizeForAvailability","RoundRobin_OptimizeForEqualDistribution"],"default":"RoundRobin_OptimizeForAvailability"},"name":{"type":"string"},"description":{"type":"string"},"slug":{"type":"string"},"widgetSlug":{"type":"string"},"calendarType":{"type":"string","enum":["round_robin","event","class_booking","collective","service_booking","personal"]},"widgetType":{"type":"string","description":"Calendar widget type. Choose \"default\" for \"neo\" and \"classic\" for \"classic\" layout.","enum":["default","classic"],"default":"classic"},"eventTitle":{"type":"string","default":"{{contact.name}}"},"eventColor":{"type":"string","default":"#039be5"},"meetingLocation":{"type":"string","description":"🚨 Deprecated! Use `locationConfigurations.location` or `teamMembers[].locationConfigurations.location` instead."},"locationConfigurations":{"description":"Meeting location configuration for event calendar","type":"array","items":{"type":"object","properties":{"kind":{"type":"string","description":"Type of meeting location. zoom_conference/google_conference/ms_teams_conference is not supported in event calendar type","enum":["custom","zoom_conference","google_conference","inbound_call","outbound_call","physical","booker","ms_teams_conference"]},"location":{"type":"string","description":"Address for meeting location. Not applicable on \"zoom_conference\", \"google_conference\" and \"ms_teams_conference\" kind"}},"required":["kind"]}},"slotDuration":{"type":"number","description":"This controls the duration of the meeting","default":30},"slotDurationUnit":{"type":"string","description":"Unit for slot duration.","enum":["mins","hours"]},"slotInterval":{"type":"number","description":"Slot interval reflects the amount of time the between booking slots that will be shown in the calendar.","default":30},"slotIntervalUnit":{"type":"string","description":"Unit for slot interval.","enum":["mins","hours"]},"slotBuffer":{"type":"number","description":"Slot-Buffer is additional time that can be added after an appointment, allowing for extra time to wrap up"},"slotBufferUnit":{"type":"string","description":"Unit for slot buffer.","enum":["mins","hours"]},"preBuffer":{"type":"number","description":"Pre-Buffer is additional time that can be added before an appointment, allowing for extra time to get ready"},"preBufferUnit":{"type":"string","description":"Unit for pre-buffer.","enum":["mins","hours"]},"appoinmentPerSlot":{"type":"number","default":1,"description":"Maximum bookings per slot (per user). Maximum seats per slot in case of Class Booking Calendar."},"appoinmentPerDay":{"type":"number","description":"Number of appointments that can be booked for a given day"},"allowBookingAfter":{"type":"number","description":"Minimum scheduling notice for events"},"allowBookingAfterUnit":{"type":"string","description":"Unit for minimum scheduling notice","enum":["hours","days","weeks","months"]},"allowBookingFor":{"type":"number","description":"Minimum number of days/weeks/months for which to allow booking events"},"allowBookingForUnit":{"type":"string","description":"Unit for controlling the duration for which booking would be allowed for","enum":["days","weeks","months"]},"openHours":{"description":"This is only to set the standard availability. For custom availability, use the availabilities property","type":"array","items":{"type":"object","properties":{"daysOfTheWeek":{"type":"array","items":{"type":"number","maximum":6,"minimum":0}},"hours":{"type":"array","items":{"type":"object","properties":{"openHour":{"type":"number","minimum":0,"maximum":23},"openMinute":{"type":"number","minimum":0,"maximum":60},"closeHour":{"type":"number","minimum":0,"maximum":23},"closeMinute":{"type":"number","minimum":0,"maximum":60}},"required":["openHour","openMinute","closeHour","closeMinute"]}}},"required":["daysOfTheWeek","hours"]}},"enableRecurring":{"type":"boolean","description":"Enable recurring appointments for the calendars. Please note that only one member should be added in the calendar to enable this","default":false},"recurring":{"type":"object","properties":{"freq":{"type":"string","enum":["DAILY","WEEKLY","MONTHLY"]},"count":{"type":"number","description":"Number of recurrences","maximum":24},"bookingOption":{"type":"string","description":"This setting contols what to do incase a recurring slot is unavailable","enum":["skip","continue","book_next"]},"bookingOverlapDefaultStatus":{"type":"string","description":"This setting contols what to do incase a recurring slot is unavailable","enum":["confirmed","new"]}}},"formId":{"type":"string"},"stickyContact":{"type":"boolean"},"isLivePaymentMode":{"type":"boolean"},"autoConfirm":{"type":"boolean","default":true},"shouldSendAlertEmailsToAssignedMember":{"type":"boolean"},"alertEmail":{"type":"string"},"googleInvitationEmails":{"type":"boolean","default":false},"allowReschedule":{"type":"boolean","default":true},"allowCancellation":{"type":"boolean","default":true},"shouldAssignContactToTeamMember":{"type":"boolean"},"shouldSkipAssigningContactForExisting":{"type":"boolean"},"notes":{"type":"string"},"pixelId":{"type":"string"},"formSubmitType":{"type":"string","default":"ThankYouMessage","enum":["RedirectURL","ThankYouMessage"]},"formSubmitRedirectURL":{"type":"string"},"formSubmitThanksMessage":{"type":"string"},"availabilityType":{"type":"number","description":"Determines which availability type to consider:\n- **1**: Only custom availabilities will be used.\n- **0**: Only open hours will be used.\n- **null**: Both custom availabilities and open hours will be considered.","enum":[0,1]},"availabilities":{"description":"This is only to set the custom availability. For standard availability, use the openHours property","type":"array","items":{"type":"object","properties":{"date":{"type":"string","description":"Formulate the date string in the format of `<YYYY-MM-DD in local timezone>T00:00:00.000Z`."},"hours":{"type":"array","items":{"type":"object","properties":{"openHour":{"type":"number","minimum":0,"maximum":23},"openMinute":{"type":"number","minimum":0,"maximum":60},"closeHour":{"type":"number","minimum":0,"maximum":23},"closeMinute":{"type":"number","minimum":0,"maximum":60}},"required":["openHour","openMinute","closeHour","closeMinute"]}},"deleted":{"type":"boolean","default":false}},"required":["date","hours"]}},"guestType":{"type":"string","enum":["count_only","collect_detail"]},"consentLabel":{"type":"string"},"calendarCoverImage":{"type":"string"},"lookBusyConfig":{"description":"Look Busy Configuration","allOf":[{"type":"object","properties":{"enabled":{"type":"boolean","description":"Apply Look Busy","example":true,"default":false},"LookBusyPercentage":{"type":"number","description":"Percentage of slots that will be hidden"}},"required":["enabled","LookBusyPercentage"]}]}},"required":["locationId","name"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/calendars",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["calendars.write"]}]
  }],
  ["get-campaigns", {
    name: "get-campaigns",
    description: `Get Campaigns`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"status":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/campaigns",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"status","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["campaigns.readonly"]}]
  }],
  ["get-contacts", {
    name: "get-contacts",
    description: `Get Contacts

 **Note:** This API endpoint is deprecated. Please use the [Search Contacts](https://highlevel.stoplight.io/docs/integrations/dbe4f3a00a106-search-contacts) endpoint instead.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Location Id"},"startAfterId":{"type":"string","description":"Start After Id"},"startAfter":{"type":"number","description":"Start Afte"},"query":{"type":"string","description":"Contact Query"},"limit":{"default":20,"type":"number","description":"Limit Per Page records count. will allow maximum up to 100 and default will be 20"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/contacts",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"startAfterId","in":"query"},{"name":"startAfter","in":"query"},{"name":"query","in":"query"},{"name":"limit","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["contacts.readonly"]}]
  }],
  ["create-contact", {
    name: "create-contact",
    description: `Please find the list of acceptable values for the \`country\` field  <a href="https://highlevel.stoplight.io/docs/integrations/ZG9jOjI4MzUzNDIy-country-list" target="_blank">here</a>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"firstName":{"type":["string","null"]},"lastName":{"type":["string","null"]},"name":{"type":["string","null"]},"email":{"type":["string","null"]},"locationId":{"type":"string"},"gender":{"type":"string"},"phone":{"type":["string","null"]},"address1":{"type":["string","null"]},"city":{"type":["string","null"]},"state":{"type":["string","null"]},"postalCode":{"type":"string"},"website":{"type":["string","null"]},"timezone":{"type":["string","null"]},"dnd":{"type":"boolean"},"dndSettings":{"type":"object","properties":{"Call":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"Email":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"SMS":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"WhatsApp":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"GMB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]},"FB":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive","permanent"]},"message":{"type":"string"},"code":{"type":"string"}},"required":["status"]}}},"inboundDndSettings":{"type":"object","properties":{"all":{"type":"object","properties":{"status":{"type":"string","enum":["active","inactive"]},"message":{"type":"string"}},"required":["status"]}}},"tags":{"type":"array","items":{"type":"string"}},"customFields":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Text"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"string","example":"My Selected Option"}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":100.5}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":{"f31175d4-2b06-4fc6-b7bc-74cd814c68cb":{"meta":{"fieldname":"1HeGizb13P0odwgOgKSs","originalname":"IMG_20231215_164412935.jpg","encoding":"7bit","mimetype":"image/jpeg","size":1786611,"uuid":"f31175d4-2b06-4fc6-b7bc-74cd814c68cb"},"url":"https://services.leadconnectorhq.com/documents/download/w2M9qTZ0ZJz8rGt02jdJ","documentId":"w2M9qTZ0ZJz8rGt02jdJ"}}}},"required":["id"]}]}},"source":{"type":"string"},"country":{"type":"string"},"companyName":{"type":["string","null"]},"assignedTo":{"type":"string","description":"User's Id"}},"required":["locationId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/contacts",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["contacts.write"]}]
  }],
  ["create-conversation", {
    name: "create-conversation",
    description: `Creates a new conversation with the data provided`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-04-15"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string","description":"Location ID as string"},"contactId":{"type":"string","description":"Contact ID as string"}},"required":["locationId","contactId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/conversations",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["conversations.write"]}]
  }],
  ["create-custom-field-2", {
    name: "create-custom-field-2",
    description: `<div>
                  <p> Create Custom Field </p> 
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                        Only supports Custom Objects and Company (Business) today. Will be extended to other Standard Objects in the future.
                        </strong>
                      </span>
                  </div>
                </div>`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"name":{"type":"string"},"dataType":{"type":"string"},"placeholder":{"type":"string"},"acceptedFormat":{"type":"array","items":{"type":"string"}},"isMultipleFile":{"type":"boolean"},"maxNumberOfFiles":{"type":"number"},"textBoxListOptions":{"type":"array","items":{"anyOf":[{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}},{"type":"object","properties":{"label":{"type":"string","example":"First"},"prefillValue":{"type":"string","example":""}}}]}},"position":{"type":"number","default":0},"model":{"type":"string","description":"Model of the custom field you want to create","enum":["contact","opportunity"]}},"required":["name","dataType"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/custom-fields",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["locations/customFields.write"]}]
  }],
  ["get-custom-menus", {
    name: "get-custom-menus",
    description: `Fetches a collection of custom menus based on specified criteria. This endpoint allows clients to retrieve custom menu configurations, which may include menu items, categories, and associated metadata. The response can be tailored using query parameters for filtering, sorting, and pagination.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"Unique identifier of the location"},"skip":{"minimum":0,"default":0,"type":"number","description":"Number of items to skip for pagination"},"limit":{"minimum":1,"default":20,"type":"number","description":"Maximum number of items to return"},"query":{"type":"string","description":"Search query to filter custom menus by name, supports partial || full names"},"showOnCompany":{"type":"boolean","description":"Filter to show only agency-level menu links. When omitted, fetches both agency and sub-account menu links. Ignored if locationId is provided"}},"required":["Version"]},
    method: "get",
    pathTemplate: "/custom-menus",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"query","in":"query"},{"name":"showOnCompany","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":["custom-menu-link.readonly"]}]
  }],
  ["create-custom-menu", {
    name: "create-custom-menu",
    description: `Creates a new custom menu for a company. Requires authentication and proper permissions. For Icon Usage Details please refer to  https://doc.clickup.com/8631005/d/h/87cpx-243696/d60fa70db6b92b2`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"title":{"type":"string","description":"Title of the custom menu"},"url":{"type":"string","description":"URL of the custom menu"},"icon":{"description":"Icon information for the custom menu","allOf":[{"type":"object","properties":{"name":{"type":"string","description":"Name of the icon","example":"yin-yang"},"fontFamily":{"type":"string","description":"Font family of the icon","enum":["fab","fas","far"]}},"required":["name","fontFamily"]}]},"showOnCompany":{"type":"boolean","description":"Whether the menu must be displayed on the agency's level","default":true},"showOnLocation":{"type":"boolean","description":"Whether the menu must be displayed for sub-accounts level","default":true},"showToAllLocations":{"type":"boolean","description":"Whether the menu must be displayed to all sub-accounts","default":true},"openMode":{"type":"string","description":"Mode for opening the menu link","enum":["iframe","new_tab","current_tab"]},"locations":{"description":"List of sub-account IDs where the menu should be shown. This list is applicable only when showOnLocation is true and showToAllLocations is false","type":"array","items":{"type":"string"}},"userRole":{"type":"string","description":"Which user-roles should the menu be accessible to?","enum":["all","admin","user"]},"allowCamera":{"type":"boolean","description":"Whether to allow camera access (only for iframe mode)"},"allowMicrophone":{"type":"boolean","description":"Whether to allow microphone access (only for iframe mode)"}},"required":["title","url","icon","showOnCompany","showOnLocation","showToAllLocations","openMode","locations","userRole"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/custom-menus",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["custom-menu-link.write"]}]
  }],
  ["get-forms", {
    name: "get-forms",
    description: `Get Forms`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"skip":{"type":"number"},"limit":{"default":10,"type":"number","description":"Limit Per Page records count. will allow maximum up to 50 and default will be 10"},"type":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/forms",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"type","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["forms.readonly"]}]
  }],
  ["list-invoices", {
    name: "list-invoices",
    description: `API to get list of invoices`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"enum":["location"],"type":"string","description":"Alt Type"},"status":{"type":"string","description":"status to be filtered"},"startAt":{"type":"string","description":"startAt in YYYY-MM-DD format"},"endAt":{"type":"string","description":"endAt in YYYY-MM-DD format"},"search":{"type":"string","description":"To search for an invoice by id / name / email / phoneNo"},"paymentMode":{"enum":["default","live","test"],"type":"string","description":"payment mode"},"contactId":{"type":"string","description":"Contact ID for the invoice"},"limit":{"type":"string","description":"Limit the number of items to return"},"offset":{"type":"string","description":"Number of items to skip"},"sortField":{"enum":["issueDate"],"type":"string","description":"The field on which sorting should be applied"},"sortOrder":{"enum":["ascend","descend"],"type":"string","description":"The order of sort which should be applied for the sortField"}},"required":["Version","altId","altType","limit","offset"]},
    method: "get",
    pathTemplate: "/invoices",
    executionParameters: [{"name":"Version","in":"header"},{"name":"altId","in":"query"},{"name":"altType","in":"query"},{"name":"status","in":"query"},{"name":"startAt","in":"query"},{"name":"endAt","in":"query"},{"name":"search","in":"query"},{"name":"paymentMode","in":"query"},{"name":"contactId","in":"query"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"sortField","in":"query"},{"name":"sortOrder","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["invoices.readonly"]},{"Agency-Access":["invoices.readonly"]}]
  }],
  ["create-invoice", {
    name: "create-invoice",
    description: `API to create an invoice`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"altId":{"type":"string","description":"location Id / company Id based on altType"},"altType":{"type":"string","description":"Alt Type","enum":["location"]},"name":{"type":"string","description":"Invoice Name"},"businessDetails":{"type":"object","properties":{"logoUrl":{"type":"string","description":"Business Logo URL"},"name":{"type":"string","description":"Business Name"},"phoneNo":{"type":"string","description":"Business Phone Number"},"address":{"description":"Business Address","allOf":[{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}}]},"website":{"type":"string","description":"Business Website Link"},"customValues":{"description":"Custom Values","type":"array","items":{"type":"string"}}}},"currency":{"type":"string","description":"Currency code"},"items":{"description":"An array of items for the invoice.","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"Invoice Item Name"},"description":{"type":"string","description":"Invoice descriptions"},"productId":{"type":"string","description":"Product Id"},"priceId":{"type":"string","description":"Price Id"},"currency":{"type":"string","description":"Currency"},"amount":{"type":"number","description":"Product amount"},"qty":{"type":"number","description":"Product Quantity"},"taxes":{"description":"Tax","type":"array","items":{"type":"object","properties":{"_id":{"type":"string"},"name":{"type":"string"},"rate":{"type":"number"},"calculation":{"type":"string","enum":["exclusive"]},"description":{"type":"string"},"taxId":{"type":"string"}},"required":["_id","name","rate"]}},"automaticTaxCategoryId":{"type":"string","description":"Tax category id for calculating automatic tax"},"isSetupFeeItem":{"type":"boolean","description":"Setupfee item, only created when 1st invoice of recurring schedule is generated"},"type":{"type":"string","description":"Price type of the item","enum":["one_time","recurring"]},"taxInclusive":{"type":"boolean","description":"true if item amount is tax inclusive","default":false}},"required":["name","currency","amount","qty"]}},"discount":{"type":"object","properties":{"id":{"type":"string","description":"Unique identifier for the discount"},"value":{"type":"number","description":"Discount value (either a percentage or custom amount)"},"type":{"type":"string","enum":["percentage","custom_amount"],"description":"Type of discount"}},"required":["id","value","type"]},"termsNotes":{"type":"string","description":"Terms notes, Also supports HTML markups"},"title":{"type":"string","description":"Title for the invoice"},"contactDetails":{"description":"Contact information to send the invoice to","allOf":[{"type":"object","properties":{"id":{"type":"string","description":"Contact ID","example":"6578278e879ad2646715ba9c"},"name":{"type":"string","description":"Contact Name","example":"Alex"},"phoneNo":{"type":"string","description":"Contact Phone Number","example":"+1234567890"},"email":{"type":"string","description":"Contact Email","example":"alex@example.com"},"additionalEmails":{"description":"Secondary email addresses for the contact to be saved","type":"array","items":{"type":"object","properties":{"email":{"type":"string","example":"alex@example.com"}},"required":["email"]}},"companyName":{"type":"string","description":"Contact Company Name","example":"ABC Corp."},"address":{"type":"object","properties":{"addressLine1":{"type":"string","description":"Address Line 1","example":"9931 Beechwood"},"addressLine2":{"type":"string","description":"Address Line 2","example":"Beechwood"},"city":{"type":"string","description":"City","example":"St. Houston"},"state":{"type":"string","description":"State","example":"TX"},"countryCode":{"type":"string","description":"Country Code","example":"US"},"postalCode":{"type":"string","description":"Postal Code","example":"559-6993"}}},"customFields":{"description":"Custom Values","type":"array","items":{"type":"string"}}},"required":["id","name","phoneNo","email"]}]},"invoiceNumber":{"type":"string","description":"Invoice Number"},"issueDate":{"type":"string","description":"Issue date in YYYY-MM-DD format"},"dueDate":{"type":"string","description":"Due date in YYYY-MM-DD format"},"sentTo":{"type":"object","properties":{"email":{"description":"Email Address","type":"array","items":{"type":"string"}},"emailCc":{"description":"cc to be kept in any sent out emails","type":"array","items":{"type":"string"}},"emailBcc":{"description":"bcc to be kept in any sent out emails","type":"array","items":{"type":"string"}},"phoneNo":{"description":"Contact Phone Number","type":"array","items":{"type":"string"}}},"required":["email"]},"liveMode":{"type":"boolean"},"automaticTaxesEnabled":{"type":"boolean","description":"Automatic taxes enabled for the Invoice"},"paymentSchedule":{"description":"split invoice into payment schedule summing up to full invoice amount","allOf":[{"type":"object","properties":{"type":{"type":"string","description":"Payment schedule type","enum":["fixed","percentage"],"example":"percentage"},"schedules":{"description":"payment schedule item","type":"array","items":{"type":"string"}}},"required":["type","schedules"]}]},"lateFeesConfiguration":{"description":"late fees configuration","allOf":[{"type":"object","properties":{"enable":{"type":"boolean","description":"Enable late fees","example":true},"value":{"type":"number","description":"Late Fees Value","example":10},"type":{"type":"string","description":"Late Fees Type","example":"fixed","enum":["fixed","percentage"]},"frequency":{"description":"Late Fees Frequency","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees interval count","example":10},"interval":{"type":"string","description":"Late fees interval","example":"day","enum":["minute","hour","day","week","month","one_time"]}},"required":["interval"]}]},"grace":{"description":"Late Fees Grace","allOf":[{"type":"object","properties":{"intervalCount":{"type":"number","description":"Late fees grace interval count","example":10},"interval":{"type":"string","description":"Late fees grace interval","example":"day","enum":["day"]}},"required":["intervalCount","interval"]}]},"maxLateFees":{"description":"Max late fees payable","allOf":[{"type":"object","properties":{"type":{"type":"string","example":"fixed","enum":["fixed"]},"value":{"type":"number","example":"Max late fees to pay","description":"10"}},"required":["type","value"]}]}},"required":["enable","value","type","frequency"]}]},"tipsConfiguration":{"description":"tips configuration for the invoice","allOf":[{"type":"object","properties":{"tipsPercentage":{"description":"Percentage of tips allowed","example":[5,10,15],"type":"array","items":{"type":"string"}},"tipsEnabled":{"type":"boolean","description":"Tips enabled status","example":true}},"required":["tipsPercentage","tipsEnabled"]}]},"invoiceNumberPrefix":{"type":"string","description":"prefix for invoice number"},"paymentMethods":{"description":"Payment Methods for Invoices","allOf":[{"type":"object","properties":{"stripe":{"description":"Payment Method","allOf":[{"type":"object","properties":{"enableBankDebitOnly":{"type":"boolean","description":"Enable Bank Debit Only","example":false}},"required":["enableBankDebitOnly"]}]}},"required":["stripe"]}]},"attachments":{"description":"attachments for the invoice","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"Id of the file selected"},"name":{"type":"string","description":"Name of the file "},"url":{"type":"string","description":"URL of the file"},"type":{"type":"string","description":"Type of the file"},"size":{"type":"number","description":"Size of the file"}},"required":["id","name","url","type","size"]}},"miscellaneousCharges":{"description":"miscellaneous charges for the invoice","allOf":[{"type":"object","properties":{"charges":{"description":"charges for the processing fee","items":{"type":"array","items":{"type":"object"}},"type":"array"},"collectedMiscellaneousCharges":{"type":"number","description":"collected miscellaneous charges","example":10},"paidCharges":{"description":"paid miscellaneous charges","type":"array","items":{"type":"object","properties":{"name":{"type":"string","description":"name of the processing fee","example":"Processing Fee"},"charge":{"type":"number","description":"charge for the processing fee","example":10},"amount":{"type":"number","description":"amount of the processing fee","example":10},"_id":{"type":"string","description":"id of the processing fee","example":"673d01d7d547648a8dab6211"}},"required":["name","charge","amount","_id"]}}},"required":["charges"]}]}},"required":["altId","altType","name","businessDetails","currency","items","discount","contactDetails","issueDate","sentTo","liveMode"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/invoices",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["invoices.write"]},{"Agency-Access":["invoices.write"]}]
  }],
  ["get-links", {
    name: "get-links",
    description: `Get Links`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/links",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["links.readonly"]}]
  }],
  ["create-link", {
    name: "create-link",
    description: `Create Link`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"locationId":{"type":"string"},"name":{"type":"string"},"redirectTo":{"type":"string"}},"required":["locationId","name","redirectTo"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/links",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["links.write"]}]
  }],
  ["create-location", {
    name: "create-location",
    description: `<div>
                  <p>Create a new Sub-Account (Formerly Location) based on the data provided</p> 
                  <div>
                    <span style= "display: inline-block;
                                width: 25px; height: 25px;
                                background-color: yellow;
                                color: black;
                                font-weight: bold;
                                font-size: 24px;
                                text-align: center;
                                line-height: 22px;
                                border: 2px solid black;
                                border-radius: 10%;
                                margin-right: 10px;">
                                !
                      </span>
                      <span>
                        <strong>
                          This feature is only available on Agency Pro ($497) plan.
                        </strong>
                      </span>
                  </div>
                </div>
    `,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name for the sub-account/location"},"phone":{"type":"string","description":"The phone number of the business for which sub-account is created with the appropriate country-code"},"companyId":{"type":"string","description":"Company/Agency Id"},"address":{"type":"string","description":"The address of the business for which sub-account is created"},"city":{"type":"string","description":"The city where the business is located for which sub-account is created"},"state":{"type":"string","description":"The state in which the business operates for which sub-account is created"},"country":{"type":"string","description":"The 2 letter country-code in which the business is present for which sub-account is created","enum":["AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR","XK","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","ME","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","MF","PM","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SX","SK","SI","SB","SO","ZA","GS","ES","LK","SD","SR","SJ","SZ","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","GB","UA","AE","US","UM","UY","UZ","VU","VE","VN","VG","VI","WF","EH","YE","ZM","ZW"]},"postalCode":{"type":"string","description":"The postal code of the business for which sub-account is created"},"website":{"type":"string","description":"The website of the business for which sub-account is created"},"timezone":{"type":"string","description":"The timezone of the business for which sub-account is created"},"prospectInfo":{"allOf":[{"type":"object","properties":{"firstName":{"type":"string","description":"First name of the prospect","example":"John"},"lastName":{"type":"string","description":"Last name of the prospect","example":"Doe"},"email":{"type":"string","description":"Email of the prospect","example":"john.doe@mail.com"}},"required":["firstName","lastName","email"]}]},"settings":{"description":"The default settings for location","allOf":[{"type":"object","properties":{"allowDuplicateContact":{"type":"boolean","example":false},"allowDuplicateOpportunity":{"type":"boolean","example":false},"allowFacebookNameMerge":{"type":"boolean","example":false},"disableContactTimezone":{"type":"boolean","example":false}}}]},"social":{"description":"The social media links for location","allOf":[{"type":"object","properties":{"facebookUrl":{"type":"string","description":"Facebook URL","example":"https://www.facebook.com/"},"googlePlus":{"type":"string","description":"Googleplus URL","example":"https://www.googleplus.com/"},"linkedIn":{"type":"string","description":"LinkedIn URL","example":"https://www.linkedIn.com/"},"foursquare":{"type":"string","description":"Foursquare URL","example":"https://www.foursquare.com/"},"twitter":{"type":"string","description":"Twitter URL","example":"https://www.foutwitterrsquare.com/"},"yelp":{"type":"string","description":"Yelp URL","example":"https://www.yelp.com/"},"instagram":{"type":"string","description":"Instagram URL","example":"https://www.instagram.com/"},"youtube":{"type":"string","description":"Instagram URL","example":"https://www.youtube.com/"},"pinterest":{"type":"string","description":"Instagram URL","example":"https://www.pinterest.com/"},"blogRss":{"type":"string","description":"Instagram URL","example":"https://www.blogRss.com/"},"googlePlacesId":{"type":"string","description":"Google Business Places ID","example":"ChIJJGPdVbQTrjsRGUkefteUeFk"}}}]},"twilio":{"description":"The twilio credentials for location","allOf":[{"type":"object","properties":{"sid":{"type":"string","description":"SID provided by Twilio","example":"AC_XXXXXXXXXXX"},"authToken":{"type":"string","description":"Auth token provided by Twilio","example":"77_XXXXXXXXXXX"}},"required":["sid","authToken"]}]},"mailgun":{"description":"The mailgun credentials for location","allOf":[{"type":"object","properties":{"apiKey":{"type":"string","description":"API key provided by Mailgun","example":"key-XXXXXXXXXXX"},"domain":{"type":"string","description":"Domain connected with Mailgun","example":"replies.yourdomain.com"}},"required":["apiKey","domain"]}]},"snapshotId":{"type":"string","description":"The snapshot ID to be loaded into the location."}},"required":["name","companyId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/locations",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["locations.write"]}]
  }],
  ["get-object-by-location-id", {
    name: "get-object-by-location-id",
    description: `Get all objects for a location. Supported Objects are contact, opportunity, business and custom objects.To understand objects and records, please have a look at the documentation here : https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string","description":"location id"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/objects",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["objects/schema.readonly"]}]
  }],
  ["create-custom-object-schema", {
    name: "create-custom-object-schema",
    description: `Allows you to create a custom object schema. To understand objects and records, please have a look at the documentation here : https://doc.clickup.com/8631005/d/h/87cpx-277156/93bf0c2e23177b0`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"labels":{"description":"This is what your custom object will be called. These labels will be used to display your custom object on the UI","allOf":[{"type":"object","properties":{"singular":{"type":"string","example":"Pet","description":"Singular name of the custom object"},"plural":{"type":"string","example":"Pets","description":"Plural name of the custom object"}},"required":["singular","plural"]}]},"key":{"type":"string","description":"key that would be used to refer the Custom Object internally (lowercase + underscore_separated). 'custom_objects.' would be added as prefix by default"},"description":{"type":"string","description":"Pet Object`s description"},"locationId":{"type":"string","description":"Location Id"},"primaryDisplayPropertyDetails":{"description":"Primary property which will be displayed on the record page","allOf":[{"type":"object","properties":{"key":{"type":"string","example":"custom_objects.pet.name","description":"key that would be used to refer the custom field internally (lowercase + underscore_separated). 'custom_objects.{{objectKey}}' would be added as prefix by default is not passed"},"name":{"type":"string","example":"Pet name","description":"Name of the Primary property name which will be displayed on the record page"},"dataType":{"type":"string","example":"TEXT","description":"Primary property data Type (it can either be TEXT or NUMERICAL type)"}},"required":["key","name","dataType"]}]}},"required":["labels","key","locationId","primaryDisplayPropertyDetails"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/objects",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["objects/schema.write"]}]
  }],
  ["create-opportunity", {
    name: "create-opportunity",
    description: `Create Opportunity`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"pipelineId":{"type":"string","description":"pipeline Id"},"locationId":{"type":"string"},"name":{"type":"string"},"pipelineStageId":{"type":"string"},"status":{"type":"string","enum":["open","won","lost","abandoned","all"]},"contactId":{"type":"string"},"monetaryValue":{"type":"number"},"assignedTo":{"type":"string"},"customFields":{"type":"array","description":"Add custom fields to opportunities.","items":{"anyOf":[{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ","description":"Pass either `id` or `key` of custom field"},"key":{"type":"string","example":"my_custom_field","description":"Pass either `id` or `key` of custom field"},"field_value":{"type":"string","example":"9039160788"}}},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"example":["test","test2"],"type":"array","items":{"type":"string"}}},"required":["id"]},{"type":"object","properties":{"id":{"type":"string","example":"6dvNaf7VhkQ9snc5vnjJ"},"key":{"type":"string","example":"my_custom_field"},"field_value":{"type":"object","example":{}}},"required":["id"]}]}}},"required":["pipelineId","locationId","name","status","contactId"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/opportunities",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"bearer":["opportunities.write"]}]
  }],
  ["list-invoices-2", {
    name: "list-invoices-2",
    description: `The "List Products" API allows to retrieve a paginated list of products. Customize your results by filtering products based on name or paginate through the list using the provided query parameters. This endpoint provides a straightforward way to explore and retrieve product information.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"limit":{"default":0,"type":"number","description":"The maximum number of items to be included in a single page of results"},"offset":{"default":0,"type":"number","description":"The starting index of the page, indicating the position from which the results should be retrieved."},"locationId":{"type":"string","description":"LocationId is the id of the sub-account"},"search":{"type":"string","description":"The name of the product for searching."},"collectionIds":{"type":"string","description":"Filter by product category Ids. Supports comma separated values"},"collectionSlug":{"type":"string","description":"The slug value of the collection by which the collection would be searched"},"expand":{"type":"array","items":{"type":"string"},"description":"Name of an entity whose data has to be fetched along with product. Possible entities are tax, stripe and paypal. If not mentioned, only ID will be returned in case of taxes"},"productIds":{"type":"array","items":{"type":"string"},"description":"List of product ids to be fetched."},"storeId":{"type":"string","description":"fetch and project products based on the storeId"},"includedInStore":{"type":"boolean","description":"Separate products by which are included in the store and which are not"},"availableInStore":{"type":"boolean","description":"If the product is included in the online store"},"sortOrder":{"enum":["asc","desc"],"type":"string","description":"The order of sort which should be applied for the date"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/products",
    executionParameters: [{"name":"Version","in":"header"},{"name":"limit","in":"query"},{"name":"offset","in":"query"},{"name":"locationId","in":"query"},{"name":"search","in":"query"},{"name":"collectionIds","in":"query"},{"name":"collectionSlug","in":"query"},{"name":"expand","in":"query"},{"name":"productIds","in":"query"},{"name":"storeId","in":"query"},{"name":"includedInStore","in":"query"},{"name":"availableInStore","in":"query"},{"name":"sortOrder","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["products.readonly"]},{"Agency-Access":["products.readonly"]}]
  }],
  ["create-product", {
    name: "create-product",
    description: `The "Create Product" API allows adding a new product to the system. Use this endpoint to create a product with the specified details. Ensure that the required information is provided in the request payload.`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"name":{"type":"string","description":"The name of the product."},"locationId":{"type":"string","description":"The unique identifier for the location."},"description":{"type":"string","description":"A brief description of the product."},"productType":{"type":"string","enum":["DIGITAL","PHYSICAL","SERVICE","PHYSICAL/DIGITAL"]},"image":{"type":"string","description":"The URL for the product image."},"statementDescriptor":{"type":"string","description":"The statement descriptor for the product."},"availableInStore":{"type":"boolean","description":"Indicates whether the product is available in-store."},"medias":{"description":"An array of medias for the product.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"The unique identifier for the media."},"title":{"type":"string","description":"The title of the media file."},"url":{"type":"string","description":"The URL where the media file is stored."},"type":{"type":"string","description":"The type of the media file (e.g., image, video will be supporting soon).","enum":["image","video"]},"isFeatured":{"type":"boolean","description":"Indicates whether the media is featured."},"priceIds":{"description":"Mongo ObjectIds of the prices for which the media is assigned","type":"array","items":{"type":"array","items":{"type":"object"}}}},"required":["id","url","type"]}},"variants":{"description":"An array of variants for the product.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"A unique identifier for the variant."},"name":{"type":"string","description":"The name of the variant."},"options":{"description":"An array of options for the variant.","type":"array","items":{"type":"object","properties":{"id":{"type":"string","description":"The unique identifier for the option."},"name":{"type":"string","description":"The name of the option."}},"required":["id","name"]}}},"required":["id","name","options"]}},"collectionIds":{"description":"An array of category Ids for the product","type":"array","items":{"type":"string"}},"isTaxesEnabled":{"type":"boolean","description":"Are there any taxes attached to the product. If this is true, taxes array cannot be empty.","default":false},"taxes":{"description":"List of ids of Taxes attached to the Product. If taxes are passed, isTaxesEnabled should be true.","type":"array","items":{"type":"string"}},"automaticTaxCategoryId":{"type":"string","description":"Tax category ID for Automatic taxes calculation."},"isLabelEnabled":{"type":"boolean","description":"Is the product label enabled. If this is true, label object cannot be empty.","default":false},"label":{"description":"Details for Product Label","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"The content for the product label.","example":"Featured"},"startDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format","example":"2024-06-26T05:43:35.000Z"},"endDate":{"type":"string","description":"Start date in YYYY-MM-DDTHH:mm:ssZ format","example":"2024-06-30T05:43:39.000Z"}},"required":["title"]}]},"slug":{"type":"string","description":"The slug using which the product navigation will be handled"},"seo":{"description":"SEO data for the product that will be displayed in the preview","allOf":[{"type":"object","properties":{"title":{"type":"string","description":"SEO title","example":"Best Product - Buy Now"},"description":{"type":"string","description":"SEO description","example":"This is the best product you can buy online with amazing features and great value"}}}]},"taxInclusive":{"type":"boolean","description":"Whether the taxes should be included in the purchase price","default":false}},"required":["name","locationId","productType"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/products",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Location-Access":["products.write"]},{"Agency-Access":["products.write"]}]
  }],
  ["get-custom-snapshots", {
    name: "get-custom-snapshots",
    description: `Get a list of all own and imported Snapshots`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"companyId":{"type":"string","description":"Company Id"}},"required":["Version","companyId"]},
    method: "get",
    pathTemplate: "/snapshots",
    executionParameters: [{"name":"Version","in":"header"},{"name":"companyId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Agency-Access":[]}]
  }],
  ["get-surveys", {
    name: "get-surveys",
    description: `Get Surveys`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"},"skip":{"type":"number"},"limit":{"default":10,"type":"number","description":"Limit Per Page records count. will allow maximum up to 50 and default will be 10"},"type":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/surveys",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"},{"name":"skip","in":"query"},{"name":"limit","in":"query"},{"name":"type","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["surveys.readonly"]}]
  }],
  ["get-user-by-location", {
    name: "get-user-by-location",
    description: `Get User by Location`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/users",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"Location-Access":["users.readonly"]}]
  }],
  ["create-user", {
    name: "create-user",
    description: `Create User`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"requestBody":{"type":"object","properties":{"companyId":{"type":"string"},"firstName":{"type":"string"},"lastName":{"type":"string"},"email":{"type":"string"},"password":{"type":"string"},"phone":{"type":"string"},"type":{"type":"string"},"role":{"type":"string"},"locationIds":{"type":"array","items":{"type":"string"}},"permissions":{"type":"object","properties":{"campaignsEnabled":{"type":"boolean","default":true},"campaignsReadOnly":{"type":"boolean","default":false},"contactsEnabled":{"type":"boolean","default":true},"workflowsEnabled":{"type":"boolean","default":true},"workflowsReadOnly":{"type":"boolean","default":false},"triggersEnabled":{"type":"boolean","default":true},"funnelsEnabled":{"type":"boolean","default":true},"websitesEnabled":{"type":"boolean","default":false},"opportunitiesEnabled":{"type":"boolean","default":true},"dashboardStatsEnabled":{"type":"boolean","default":true},"bulkRequestsEnabled":{"type":"boolean","default":true},"appointmentsEnabled":{"type":"boolean","default":true},"reviewsEnabled":{"type":"boolean","default":true},"onlineListingsEnabled":{"type":"boolean","default":true},"phoneCallEnabled":{"type":"boolean","default":true},"conversationsEnabled":{"type":"boolean","default":true},"assignedDataOnly":{"type":"boolean","default":false},"adwordsReportingEnabled":{"type":"boolean","default":false},"membershipEnabled":{"type":"boolean","default":false},"facebookAdsReportingEnabled":{"type":"boolean","default":false},"attributionsReportingEnabled":{"type":"boolean","default":false},"settingsEnabled":{"type":"boolean","default":true},"tagsEnabled":{"type":"boolean","default":true},"leadValueEnabled":{"type":"boolean","default":true},"marketingEnabled":{"type":"boolean","default":true},"agentReportingEnabled":{"type":"boolean","default":true},"botService":{"type":"boolean","default":false},"socialPlanner":{"type":"boolean","default":true},"bloggingEnabled":{"type":"boolean","default":true},"invoiceEnabled":{"type":"boolean","default":true},"affiliateManagerEnabled":{"type":"boolean","default":true},"contentAiEnabled":{"type":"boolean","default":true},"refundsEnabled":{"type":"boolean","default":true},"recordPaymentEnabled":{"type":"boolean","default":true},"cancelSubscriptionEnabled":{"type":"boolean","default":true},"paymentsEnabled":{"type":"boolean","default":true},"communitiesEnabled":{"type":"boolean","default":true},"exportPaymentsEnabled":{"type":"boolean","default":true}}},"scopes":{"type":"array","description":"Scopes allowed for users. Only scopes that have been passed will be enabled. Note:- If passed empty all the scopes will be get disabled","items":{"type":"string","enum":["campaigns.readonly","campaigns.write","calendars.readonly","calendars/events.write","calendars/groups.write","calendars.write","contacts.write","contacts/bulkActions.write","workflows.readonly","workflows.write","triggers.write","funnels.write","forms.write","surveys.write","quizzes.write","websites.write","medias.write","medias.readonly","opportunities.write","opportunities/leadValue.readonly","opportunities/bulkActions.write","reporting/phone.readonly","reporting/adwords.readonly","reporting/facebookAds.readonly","reporting/attributions.readonly","prospecting/auditReport.write","reporting/reports.readonly","reporting/agent.readonly","reporting/reports.write","payments.write","payments/refunds.write","payments/records.write","payments/exports.write","payments/subscriptionsCancel.write","invoices.write","invoices.readonly","invoices/schedule.readonly","invoices/schedule.write","invoices/template.readonly","invoices/template.write","reputation/review.write","reputation/listing.write","reputation/reviewsAIAgents.write","conversations.write","conversations.readonly","conversations/message.readonly","conversations/message.write","contentAI.write","dashboard/stats.readonly","locations/tags.write","locations/tags.readonly","marketing.write","eliza.write","settings.write","socialplanner/post.write","socialplanner/account.readonly","socialplanner/account.write","socialplanner/category.readonly","socialplanner/category.write","socialplanner/csv.readonly","socialplanner/csv.write","socialplanner/group.write","socialplanner/hashtag.readonly","socialplanner/hashtag.write","socialplanner/oauth.readonly","socialplanner/oauth.write","socialplanner/post.readonly","socialplanner/recurring.readonly","socialplanner/recurring.write","socialplanner/review.readonly","socialplanner/review.write","socialplanner/rss.readonly","socialplanner/rss.write","socialplanner/search.readonly","socialplanner/setting.readonly","socialplanner/setting.write","socialplanner/stat.readonly","socialplanner/tag.readonly","socialplanner/tag.write","socialplanner/filters.readonly","socialplanner/medias.readonly","socialplanner/medias.write","socialplanner/watermarks.readonly","socialplanner/watermarks.write","socialplanner/metatag.readonly","socialplanner/facebook.readonly","socialplanner/linkedin.readonly","socialplanner/twitter.readonly","socialplanner/notification.readonly","socialplanner/notification.write","socialplanner/snapshot.readonly","socialplanner/snapshot.write","marketing/affiliate.write","blogs.write","membership.write","communities.write","gokollab.write","certificates.write","certificates.readonly","adPublishing.write","adPublishing.readonly","prospecting.write","prospecting.readonly","prospecting/reports.readonly","private-integration-location.readonly","private-integration-location.write","private-integration-company.readonly","private-integration-company.write","native-integrations.readonly","native-integrations.write","wordpress.write","wordpress.read","custom-menu-link.write","qrcodes.write","users/team-management.write","users/team-management.readonly","loginas.write","snapshots/api.readonly","snapshots/api.create","snapshots/api.edit","snapshots/api.push","snapshots/api.refresh","snapshots/api.share","snapshots/api.delete","internaltools.location-transfer.write","internaltools.location-transfer.readonly","affiliateportal.write","affiliateportal.readonly","companies.write","internaltools.billing.write","internaltools.billing.readonly","internaltools.billing-common.readonly","internaltools.billing-common.write","voice-ai-agents.write","voice-ai-agent-goals.readonly","voice-ai-agent-goals.write","voice-ai-dashboard.readonly","agency/launchpad.write","agency/launchpad.readonly","launchpad.write","launchpad.readonly","text-ai-agents.write","text-ai-agent-goals.readonly","text-ai-agent-goals.write","text-ai-agent-training.write"]}},"scopesAssignedToOnly":{"type":"array","description":"Assigned Scopes allowed for users. Only scopes that have been passed will be enabled. If passed empty all the assigned scopes will be get disabled","items":{"type":"string","enum":["campaigns.readonly","campaigns.write","calendars.readonly","calendars/events.write","calendars/groups.write","calendars.write","contacts.write","contacts/bulkActions.write","workflows.readonly","workflows.write","triggers.write","funnels.write","forms.write","surveys.write","quizzes.write","websites.write","medias.write","medias.readonly","opportunities.write","opportunities/leadValue.readonly","opportunities/bulkActions.write","reporting/phone.readonly","reporting/adwords.readonly","reporting/facebookAds.readonly","reporting/attributions.readonly","prospecting/auditReport.write","reporting/reports.readonly","reporting/agent.readonly","reporting/reports.write","payments.write","payments/refunds.write","payments/records.write","payments/exports.write","payments/subscriptionsCancel.write","invoices.write","invoices.readonly","invoices/schedule.readonly","invoices/schedule.write","invoices/template.readonly","invoices/template.write","reputation/review.write","reputation/listing.write","reputation/reviewsAIAgents.write","conversations.write","conversations.readonly","conversations/message.readonly","conversations/message.write","contentAI.write","dashboard/stats.readonly","locations/tags.write","locations/tags.readonly","marketing.write","eliza.write","settings.write","socialplanner/post.write","socialplanner/account.readonly","socialplanner/account.write","socialplanner/category.readonly","socialplanner/category.write","socialplanner/csv.readonly","socialplanner/csv.write","socialplanner/group.write","socialplanner/hashtag.readonly","socialplanner/hashtag.write","socialplanner/oauth.readonly","socialplanner/oauth.write","socialplanner/post.readonly","socialplanner/recurring.readonly","socialplanner/recurring.write","socialplanner/review.readonly","socialplanner/review.write","socialplanner/rss.readonly","socialplanner/rss.write","socialplanner/search.readonly","socialplanner/setting.readonly","socialplanner/setting.write","socialplanner/stat.readonly","socialplanner/tag.readonly","socialplanner/tag.write","socialplanner/filters.readonly","socialplanner/medias.readonly","socialplanner/medias.write","socialplanner/watermarks.readonly","socialplanner/watermarks.write","socialplanner/metatag.readonly","socialplanner/facebook.readonly","socialplanner/linkedin.readonly","socialplanner/twitter.readonly","socialplanner/notification.readonly","socialplanner/notification.write","socialplanner/snapshot.readonly","socialplanner/snapshot.write","marketing/affiliate.write","blogs.write","membership.write","communities.write","gokollab.write","certificates.write","certificates.readonly","adPublishing.write","adPublishing.readonly","prospecting.write","prospecting.readonly","prospecting/reports.readonly","private-integration-location.readonly","private-integration-location.write","private-integration-company.readonly","private-integration-company.write","native-integrations.readonly","native-integrations.write","wordpress.write","wordpress.read","custom-menu-link.write","qrcodes.write","users/team-management.write","users/team-management.readonly","loginas.write","snapshots/api.readonly","snapshots/api.create","snapshots/api.edit","snapshots/api.push","snapshots/api.refresh","snapshots/api.share","snapshots/api.delete","internaltools.location-transfer.write","internaltools.location-transfer.readonly","affiliateportal.write","affiliateportal.readonly","companies.write","internaltools.billing.write","internaltools.billing.readonly","internaltools.billing-common.readonly","internaltools.billing-common.write","voice-ai-agents.write","voice-ai-agent-goals.readonly","voice-ai-agent-goals.write","voice-ai-dashboard.readonly","agency/launchpad.write","agency/launchpad.readonly","launchpad.write","launchpad.readonly","text-ai-agents.write","text-ai-agent-goals.readonly","text-ai-agent-goals.write","text-ai-agent-training.write"]}},"profilePhoto":{"type":"string"}},"required":["companyId","firstName","lastName","email","password","type","role","locationIds"],"description":"The JSON request body."}},"required":["Version","requestBody"]},
    method: "post",
    pathTemplate: "/users",
    executionParameters: [{"name":"Version","in":"header"}],
    requestBodyContentType: "application/json",
    securityRequirements: [{"Agency-Access":["users.write"]},{"Location-Access":["users.write"]}]
  }],
  ["get-workflow", {
    name: "get-workflow",
    description: `Get Workflow`,
    inputSchema: {"type":"object","properties":{"Version":{"type":"string","enum":["2021-07-28"],"description":"API Version"},"locationId":{"type":"string"}},"required":["Version","locationId"]},
    method: "get",
    pathTemplate: "/workflows",
    executionParameters: [{"name":"Version","in":"header"},{"name":"locationId","in":"query"}],
    requestBodyContentType: undefined,
    securityRequirements: [{"bearer":["workflows.readonly"]}]
  }],
]);

export function listToolsForClient() {
  return Array.from(toolDefinitionMap.values()).map((def) => ({
    name: def.name,
    description: def.description,
    inputSchema: def.inputSchema,
  }));
}
