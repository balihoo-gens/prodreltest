Object.size = function(obj) {
    var size = 0, key;
    for (key in obj) {
        if (obj.hasOwnProperty(key)) size++;
    }
    return size;
};

angular.module('filters', []).
    filter('truncate', function () {
               return function (text, length, end) {
                   if (text === undefined) {
                       return "";
                   }
                   if (text === null) {
                       return "";
                   }
                   if (isNaN(length))
                       length = 10;

                   if (end === undefined)
                       end = "...";

                   if (text.length <= length || text.length - end.length <= length) {
                       return text;
                   }
                   else {
                       return String(text).substring(0, length - end.length) + end;
                   }

               };
           });

var app = angular.module('FulfillmentDashboard', ['ngRoute', 'ngSanitize', 'ngDialog', 'filters', 'angular-moment']);

toastr.options = {
    "closeButton" : true,
    "positionClass" : "toast-bottom-right",
    "timeOut" : "50000"
};

app.factory('environment', function() {
    return {};
});

app.factory('formatUtil', function() {

    function _formatWhitespace(str) {
        var map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;',
            "\n": '<br/>',
            "\t": '&nbsp;&nbsp;&nbsp;'
        };

        if(str == undefined) { return "---"; }

        return str.replace(/[&<>"'\n\t]/g, function (m) {
            return map[m];
        });
    }

    function _div(contents, classes) {
        return "<div class='" + classes + "'>" + contents + "</div>";
    }

    function _span(contents, classes) {
        return "<span class='" + classes + "'>" + contents + "</span>";
    }

    function _jsonFormat(json, divclass) {
        if (json instanceof Array) {
            var body = "";
            for (var item in json) {
                body += _div(_jsonFormat(json[item]));
            }
            return _div(body, "block " + divclass);
        }
        if (json instanceof Object) {
            var body = "";
            for (var key in json) {
                body += _div("<span><b>" + key + "</b></span> : " + _jsonFormat(json[key], "block"));
            }
            return _div(body, "block " + divclass);
        }
        if(_isJSON(json)) {
            return _jsonFormat(JSON.parse(json), "json");
        }
        if (_isString(json)) {
            return _span(_formatURLs(json), "jsonvalue");
        }

        return _span(json, "jsonvalue");
    }

    function _formatJson(jsonString) {
        if(_isJSON(jsonString)) {
            return _jsonFormat(JSON.parse(jsonString), "json");
        }
        return _span(_formatURLs(jsonString), "");
    }

    function _formatJsonBasic(jsonString) {
        if (undefined == jsonString) {
            return jsonString;
        }
        if(_isString(jsonString)) {
            try {
              jsonString = JSON.parse(jsonString)
            } catch(e) {

            }
        }

        return JSON.stringify(jsonString, undefined, 4);
    }

    function _indent(n) {
        return Array(n).join('\t');
    }

    function _formatJsonlike(str) {
        if (undefined == str) {
            return str;
        }
        var out = "";
        var indent = 0;
        for (var i = 0, len = str.length; i < len; i++) {
            var c = str[i];
            if(c == '{') {
                c += " ";
                out += "\n" + _indent(indent);
                indent++;
            }
            if(c == '}') {
                indent--;
            }

            out += c;
        }

        return _formatWhitespace(out);
    }

    function _formatURLs(param) {
        if(param === null) { return ""; }
        var urlRegex = /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/ig;

        return param.replace(urlRegex, function (url) {

            if (( url.indexOf(".jpg") > 0 )
                || ( url.indexOf(".png") > 0 )
                || ( url.indexOf(".gif") > 0 )) {
                return '<br/><div class="block"><img class="constrained" src="' + url + '">' + '<br/><a href="' + url + '">' + url + '</a>' + '</div>';
            }
            else {
                return '<a href="' + url + '">' + url + '</a>' + '<br/>';
            }
        });
    }

    function _isJSON(j) {
        if(j === null) { return false; }
        return j[0] == '{' || j[0] == '[';
    }

    function _isArray(a) {
        return a instanceof Array;
    }

    function _isString(s) {
        return typeof s === "string";
    }

    function _escapeSlash(s) {
        return s.replace("/", "++++");
    }

    function _unEscapeSlash(s) {
        return s.replace("++++", "/");
    }

    return {
        formatWhitespace: _formatWhitespace,
        div: _div,
        span: _span,
        jsonFormat: _jsonFormat,
        formatJson: _formatJson,
        formatJsonBasic: _formatJsonBasic,
        formatJsonlike: _formatJsonlike,
        formatURLs: _formatURLs,
        isJSON: _isJSON,
        isArray: _isArray,
        isString: _isString,
        escapeSlash: _escapeSlash,
        unEscapeSlash: _unEscapeSlash
    }

});

app.config(
    function($routeProvider) {
        $routeProvider
            .when("/workflow/:workflowId/run/:runId", {
                      controller : "workflowController",
                      templateUrl : "partials/workflow.html",
                      reloadOnSearch: false})
            .when("/workflow/initiate", {
                      controller : "workflowInitiationController",
                      templateUrl : "partials/workflowInitiate.html",
                      reloadOnSearch: false})
            .when("/workflow/initiate/:command", {
                      controller : "workflowInitiationController",
                      templateUrl : "partials/workflowInitiate.html",
                      reloadOnSearch: false})
            .when("/history", {
                      controller : "historyController",
                      templateUrl : "partials/history.html",
                      reloadOnSearch: false})
            .when("/workers", {
                      controller : "workersController",
                      templateUrl : "partials/workers.html",
                      reloadOnSearch: false})
            .when("/start", { redirectTo: "/history"})
            .otherwise({redirectTo : "/history"})
        ;
    }
);

app.controller('envController', function($scope, $route, $http, $location, environment) {

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            $scope.init();
        }
    );


    $scope.init = function() {
        if(environment.data) { return; }
        $http.get('workflow/environment', {})
            .success(function(data) {
                         $scope.environment = data;
                         environment.data = data;
                     })
            .error(function(error) {
                       toastr.error(error.details, error.error)
                   });
    };

    $scope.go = function(where) {
        $location.path(where);
    };

});

app.controller('historyController', function($scope, $route, $http, $location, ngDialog, formatUtil) {

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            var start = $('#startDate');
            start.datetimepicker();
            start.data("DateTimePicker").setDate(new Date(new Date().setDate(new Date().getDate()-14)));

            var end = $('#endDate');
            end.datetimepicker();
            end.data("DateTimePicker").setDate(new Date());

            $scope.getHistory();
        }
    );


    $scope.getHistory = function() {
        $scope.loading = true;
        var params = {};
        params['startDate'] = moment($('#startDate').data("DateTimePicker").getDate()).utc().format('YYYY-MM-DDTHH:mm:ss')+"Z";
        params['endDate'] = moment($('#endDate').data("DateTimePicker").getDate()).utc().format('YYYY-MM-DDTHH:mm:ss')+"Z";
        $http.get('workflow/history', { params : params})
            .success(function(data) {
                         $scope.loading = false;
                         $scope.executions = data;
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(error.details, error.error)
                   });
    };

    $scope.seeWorkflow = function(execution) {
        $location.path("workflow/"+encodeURIComponent(execution.workflowId)+"/run/"+encodeURIComponent(execution.runId));
    };

    $scope.linkWorkflow = function(execution) {
        return "#/workflow/"+encodeURIComponent(execution.workflowId)+"/run/"+encodeURIComponent(formatUtil.escapeSlash(execution.runId));
    };

    $scope.statusMap = {
        // These are all SWF Workflow Close Statii
        "FAILED" : "label-danger",
        "COMPLETED" : "label-success",
        "TIMED_OUT" : "label-warning",
        "TERMINATED" : "label-danger",
        "CANCELED" : "label-warning",
        "CONTINUTED_AS_NEW" : "label-default",

        "IN_PROGRESS" : "label-info",
        "BLOCKED" : "label-warning",
        "CANCEL_REQUESTED" : "label-warning"

    };

    $scope.figureStatusLabel = function(status) {
        if(status == null) {
            return "label-info";
        }
        return $scope.statusMap[status];
    };

    $scope.formatStatus = function(closeStatus) {
        if(closeStatus == null) {
            return "IN_PROGRESS";
        }

        return closeStatus;
    };

    $scope.formatTag = function(tag) {
        var parts = tag.split(':');
        return "<b>"+parts[0]+"</b>&nbsp;:&nbsp;"+parts[1];
    };

    $scope.promptCancelWorkflow = function(ex) {
        $scope.ex = ex;
        ngDialog.open({
                          template : "confirmCancel",
                          controller : "historyController",
                          scope : $scope
                      });
    };

    $scope.cancelWorkflow = function() {
        var params = {};
        params['runId'] = $scope.ex.runId;
        params['workflowId'] = $scope.ex.workflowId;
        $http.post('workflow/cancel', params )
            .success(function(data) {
                         toastr.info(data, "Cancel Requested!");
                         $scope.ex.closeStatus = "CANCEL_REQUESTED";
                     })
            .error(function(error) {
                       toastr.error(error.details, error.error)
                   });

    };

    $scope.promptTerminateWorkflow = function(ex) {
        $scope.ex = ex;
        ngDialog.open({
                          template : "confirmTerminate",
                          controller : "historyController",
                          scope : $scope
                      });
    };

    $scope.terminateWorkflow = function() {
        var params = {};
        params['runId'] = $scope.ex.runId;
        params['workflowId'] = $scope.ex.workflowId;
        params['reason'] = $scope.terminateReason;
        params['details'] = $scope.terminateDetails;
        $http.post('workflow/terminate', params )
            .success(function(data) {
                         toastr.info(data, "Workflow Terminated!");
                         $scope.ex.closeStatus = "TERMINATED";
                     })
            .error(function(error) {
                       toastr.error(error.details, error.error)
                   });

    };
});


app.controller('workflowController', function($scope, $route, $http, $location, $anchorScroll, formatUtil, environment, ngDialog) {

    $scope.formatUtil = formatUtil;

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            $scope.params = $route.current.params;
            $scope.params.runId = formatUtil.unEscapeSlash($scope.params.runId);
            $scope.getWorkflow();
        }
    );


    $scope.workflow = {};

    $scope.getWorkflow = function() {
        $scope.loading = true;
        var params = {};
        params['runId'] = $scope.params.runId;
        params['workflowId'] = $scope.params.workflowId;
        $http.get('workflow/detail', { params : params})
            .success(function(data) {
                         $scope.loading = false;
                         $scope.workflow = data;
                         $scope.prepWorkflow();
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(error.details, error.error)
                   });
    };

    $scope.updateWorkflow = function() {
        $scope.loading = true;
        var params = {};
        params['runId'] = $scope.params.runId;
        params['workflowId'] = $scope.params.workflowId;
        params['input'] = angular.toJson($scope.assembleUpdates());
        $http.post('workflow/update', params )
            .success(function(data) {
                         $scope.getWorkflow();
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(error.details, error.error)
                   });

    };

    $scope.promptCancelWorkflow = function() {
        ngDialog.open({
                          template : "confirmCancel",
                          controller : "workflowController",
                          scope : $scope
                      });
    };

    $scope.cancelWorkflow = function() {
        var params = {};
        params['runId'] = $scope.params.runId;
        params['workflowId'] = $scope.params.workflowId;
        $http.post('workflow/cancel', params )
            .success(function(data) {
                         toastr.info(data, "Cancel Requested!")
                     })
            .error(function(error) {
                       toastr.error(error.details, error.error)
                   });

    };

    $scope.promptTerminateWorkflow = function() {
        ngDialog.open({
                          template : "confirmTerminate",
                          controller : "workflowController",
                          scope : $scope
                      });
    };

    $scope.terminateWorkflow = function() {
        var params = {};
        params['runId'] = $scope.params.runId;
        params['workflowId'] = $scope.params.workflowId;
        params['reason'] = $scope.terminateReason;
        params['details'] = $scope.terminateDetails;
        $http.post('workflow/terminate', params )
            .success(function(data) {
                       toastr.info(data, "Workflow Terminated!")
                     })
            .error(function(error) {
                       toastr.error(error.details, error.error)
                   });

    };

    $scope.prepWorkflow = function() {
        $scope.workflow.editable = $scope.workflow.status == "IN_PROGRESS" || $scope.workflow.status == "BLOCKED";
        $scope.workflow.edited = false;

        for(var s in $scope.workflow.sections) {
            var section = $scope.workflow.sections[s];
            section.editingStatus = false;
            section.originalStatus = section.status;
            section.showContents = false;
            for(pname in section.params) {
                var param = section.params[pname];
                if(formatUtil.isJSON(param)) {
                    param = formatUtil.formatJsonBasic(param);
                }
                section.params[pname] = {
                    "original" : param,
                    "value" : param,
                    "name" : pname,
                    "edited" : false,
                    "editing" : false,
                    "editable" : $scope.workflow.editable && section.fixable,
                    "isJson" : formatUtil.isJSON(param),
                    "isArray" : formatUtil.isArray(param),
                    "isString" : formatUtil.isString(param)
                }
            }

            section.statusChanged = function(section) {
                return section.status != section.originalStatus;
            };

            section.editStatus = function(section) {
                section.editingStatus = true;
            };

            section.changeStatus = function(section) {
                //section.status = status;
                section.editingStatus = false;
                $scope.checkForEdits();
            };

        }
    };

    $scope.cancelEdit = function(param) {
        param.value = param.original;
        param.editing = false;
        $scope.checkForEdits();
    };

    $scope.finishEditing = function(param) {
        param.editing = false;
        param.edited = param.original != param.value;
        $scope.checkForEdits();
    };

    $scope.assembleUpdates = function() {
        var updates = {};

        for(var sname in $scope.workflow.sections) {
            var section = $scope.workflow.sections[sname];
            var sectionUpdates = {};
            var paramUpdates = {};
            for (pname in section.params) {
                var param = section.params[pname];
                if(param.edited) {
                    paramUpdates[pname] = param.value;
                }
            }
            if(Object.size(paramUpdates)) {
                sectionUpdates['params'] = paramUpdates;
                sectionUpdates['status'] = "READY";
            }

            if(section.status != section.originalStatus) {
                sectionUpdates['status'] = section.status;
            }

            if(!$.isEmptyObject(sectionUpdates)) {
                updates[sname] = sectionUpdates;
            }
        }

        return updates;
    };

    $scope.checkForEdits = function() {
        $scope.workflow.edited = !$.isEmptyObject($scope.assembleUpdates());
    };

    $scope.addSection = function(s) {
        s.push('');
    };

    $scope.showSection = function(ssection) {
        for(var sname in $scope.workflow.sections) {
            var section = $scope.workflow.sections[sname];
            section.showContents = section == ssection;
        }
    };

    $scope.cancelWorkflowUpdate = function() {

        for(var sname in $scope.workflow.sections) {
            var section = $scope.workflow.sections[sname];
            section.status = section.originalStatus;
            for (pname in section.params) {
                var param = section.params[pname];
                param.value = param.original;
                param.edited = false;
            }
        }

        $scope.workflow.edited = false;
    };

    $scope.workflowStatusMap = {
        "IN_PROGRESS" : "label-info",
        "CANCEL_REQUESTED" : "label-warning",
        "BLOCKED" : "label-warning",
        "CANCELLED" : "label-warning",
        "FAILED" : "label-danger",
        "TIMED_OUT" : "label-danger",
        "TERMINATED" : "label-danger",
        "COMPLETED" : "label-success"
    };

    $scope.statusMap = {
        "READY" : "label-info",
        "SCHEDULED" : "label-info",
        "STARTED" : "label-info",
        "FAILED" : "label-warning",
        "TIMED_OUT" : "label-warning",
        "CANCELED" : "label-warning",
        "TERMINAL" : "label-danger",
        "COMPLETE" : "label-success",
        "CONTINGENT" : "label-default",
        "DEFERRED" : "label-info",
        "IMPOSSIBLE" : "label-danger"
    };

    $scope.timelineMap = {
        "NOTE" : "timeline",
        "ERROR" : "timeline-ERROR",
        "SUCCESS" : "timeline-SUCCESS",
        "WARNING" : "timeline-WARNING"
    };

    // http://fortawesome.github.io/Font-Awesome/icons/
    $scope.timelineIconMap = {
        "NOTE" : "circle",
        "ERROR" : "times-circle",
        "SUCCESS" : "check-circle",
        "WARNING" : "exclamation-circle"
    };

    $scope.figureWorkflowStatusLabel = function(status) {
        return $scope.workflowStatusMap[status];
    };

    $scope.figureStatusLabel = function(status) {
        return $scope.statusMap[status ? status : "IMPOSSIBLE"];
    };

    $scope.figureTimelineStyle = function(eventType) {
        return $scope.timelineMap[eventType];
    };

    $scope.figureTimelineIcon = function(eventType) {
        return $scope.timelineIconMap[eventType];
    };

    $scope.scrollToSection = function(name) {
        $location.hash("section_"+name);
        $anchorScroll();
    };

    $scope.newWorkflow = function() {
        environment.existingWorkflow = $scope.workflow;
        $location.path("workflow/initiate/fromExisting");
    };

    $(".rawInputToggle").click(function() {
        $('#rawInput').slideToggle()
    });

    $(".historyViewToggle").click(function() {
        $('#historyView').slideToggle()
    });
});

app.controller('workersController', function($scope, $route, $http, $location, environment, formatUtil) {

    $scope.showDefunct = false;
    $scope.formatUtil = formatUtil;

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            $scope.getWorkers();
        }
    );


    $scope.getWorkers = function() {
        $scope.loading = true;
        var params = {};
        $http.get('worker', { params : params})
            .success(function(data) {
                         $scope.loading = false;
                         $scope.workers = data;
                         $scope.categorizeWorkers();
                         $scope.currentDomain = environment.data.domain;
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(error.details, error.error)
                   });
    };

    $scope.setFreshnessLabel = function(worker) {

        worker.defunct = false;
        if(worker.minutesSinceLast < -10) {
            worker.freshness = "label-default";
            worker.defunct = true;
        } else if(worker.minutesSinceLast < -5) {
            worker.freshness = "label-warning";
        } else {
            worker.freshness = "label-success";
        }
    };

    $scope.categorizeWorkers = function() {
        $scope.domains = {};
        for(var w in $scope.workers) {
            var worker = $scope.workers[w];
            worker.showFormatted = true;
            worker.showContents = false;
            if(formatUtil.isJSON(worker.specification)) {
                worker.specification = JSON.parse(worker.specification);
            }

            if(formatUtil.isJSON(worker.resolutionHistory)) {
                worker.resolutionHistory = JSON.parse(worker.resolutionHistory);
            }

            $scope.setFreshnessLabel(worker);

            if(!$scope.domains.hasOwnProperty(worker.domain)) {
                $scope.domains[worker.domain] = {};
            }
            var domain = $scope.domains[worker.domain];
            if(!domain.hasOwnProperty(worker.activityName)) {
                domain[worker.activityName] = [];
            }

            domain[worker.activityName].push(worker);

        }
    };

});

app.controller('workflowInitiationController', function($scope, $route, $http, $location, formatUtil, environment) {

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            $scope.params = $route.current.params;
            $scope.init();
        }
    );


    $scope.init = function() {
        $scope.loading = false;
        $scope.tags = [
            { text: 'Dashboard:Initiated'}
        ];
        $scope.workflowId = "Manual Run: " + moment().unix();

        if($scope.params.command == "fromExisting") {
            if(environment.existingWorkflow !== null) {
                $scope.inputJson = JSON.stringify(JSON.parse(environment.existingWorkflow.history[0].input), null, 4);
                $scope.workflowId = "Re-run of "+environment.existingWorkflow.workflowId;
            }
        }
    };

    $scope.initiateWorkflow = function() {
        $scope.loading = true;
        var params = {};
        params['id'] = $scope.workflowId;
        params['input'] = $scope.inputJson;

        var mtags = [];
        for(var i in $scope.tags) {
            var t = $scope.tags[i];
            mtags.push(t.text);
        }
        params['tags'] = mtags.join(",");
        $http.post('workflow/initiate', params)
            .success(function(data) {
                         $scope.loading = false;
                         $scope.runId = data.runId;
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(formatUtil.formatWhitespace(error.details), error.error)
                   });
    };

    $scope.addTag = function() {
      $scope.tags.push({text : ""});
    };

    $scope.removeTag = function(tag) {
        $scope.tags.splice($scope.tags.indexOf(tag), 1);
    };

    $scope.restart = function() {
      $scope.runId = false;
    };

    $scope.seeWorkflow = function() {
        $location.path("workflow/"+encodeURIComponent($scope.workflowId)+"/run/"+encodeURIComponent($scope.runId));
    };
});
