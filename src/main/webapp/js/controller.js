Object.size = function(obj) {
    var size = 0, key;
    for (key in obj) {
        if (obj.hasOwnProperty(key)) size++;
    }
    return size;
};

var app = angular.module('FulfillmentDashboard', ['ngRoute', 'ngSanitize']);

toastr.options = {
    "closeButton" : true,
    "positionClass" : "toast-bottom-right",
    "timeOut" : "50000"
};

app.factory('environment', function() {
    return {};
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
            .when("/history", {
                      controller : "historyController",
                      templateUrl : "partials/history.html",
                      reloadOnSearch: false})
            .when("/workers", {
                      controller : "workersController",
                      templateUrl : "partials/workers.html",
                      reloadOnSearch: false})
            .when("/start", {
                      controller : "envController",
                      templateUrl : "partials/start.html",
                      reloadOnSearch: false})
            .otherwise({redirectTo : "/start"})
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

app.controller('historyController', function($scope, $route, $http, $location) {



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

    $scope.statusMap = {
        "FAILED" : "label-danger",
        "COMPLETED" : "label-success",
        "TIMED_OUT" : "label-warning",
        "TERMINATED" : "label-danger"
    };

    $scope.figureStatusLabel = function(status) {
        if(status == null) {
            return "label-info";
        }
        return $scope.statusMap[status];
    };

    $scope.formatStatus = function(closeStatus) {
        if(closeStatus == null) {
            return "IN PROGRESS";
        }

        return closeStatus;
    };

    $scope.formatTag = function(tag) {
        var parts = tag.split(':');
        return "<b>"+parts[0]+"</b>&nbsp;:&nbsp;"+parts[1];
    };
});

app.controller('workflowController', function($scope, $route, $http, $location, $anchorScroll) {

    $scope.$on(
        "$routeChangeSuccess",
        function($currentRoute, $previousRoute) {
            $scope.params = $route.current.params;
            $scope.getWorkflow($scope.params.runId, $scope.params.workflowId);
        }
    );


    $scope.workflow = {};

    $scope.getWorkflow = function(runId, workflowId) {
        $scope.loading = true;
        var params = {};
        params['runId'] = runId;
        params['workflowId'] = workflowId;
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
        params['input'] = $scope.assembleUpdates();
        $http.get('workflow/update', { params : params})
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

    $scope.prepWorkflow = function() {
        $scope.workflow.editable = $scope.workflow.resolution == "IN PROGRESS";
        $scope.workflow.edited = false;

        for(var s in $scope.workflow.sections) {
            var section = $scope.workflow.sections[s];
            for(pname in section.params) {
                var param = section.params[pname];
                if($scope.isJSON(param)) {
                    param = $scope.formatJsonBasic(param);
                }
                section.params[pname] = {
                    "original" : param,
                    "value" : param,
                    "name" : pname,
                    "edited" : false,
                    "editing" : false,
                    "editable" : $scope.workflow.editable && section.fixable,
                    "isJson" : $scope.isJSON(param),
                    "isArray" : $scope.isArray(param),
                    "isString" : $scope.isString(param)
                }
            }
        }
    };

    $scope.cancelEdit = function(param) {
        param.value = param.original;
        param.editing = false;
    };

    $scope.finishEditing = function(param) {
        param.editing = false;
        param.edited = param.original != param.value;
        $scope.workflow.edited = true;
    };

    $scope.assembleUpdates = function() {
        var updates = {};

        for(var sname in $scope.workflow.sections) {
            var section = $scope.workflow.sections[sname];
            var paramUpdates = {};
            for (pname in section.params) {
                var param = section.params[pname];
                if(param.edited) {
                    paramUpdates[pname] = param.value;
                }
            }
            if(Object.size(paramUpdates)) {
                updates[sname] = { params : paramUpdates, status : "INCOMPLETE" };
            }
        }

        return updates;
    };

    $scope.addSection = function(s) {
        s.push('');
    };

    $scope.cancelWorkflowUpdate = function() {

        for(var sname in $scope.workflow.sections) {
            var section = $scope.workflow.sections[sname];
            for (pname in section.params) {
                var param = section.params[pname];
                param.value = param.original;
                param.edited = false;
            }
        }

        $scope.workflow.edited = false;
    };

    $scope.workflowStatusMap = {
        "IN PROGRESS" : "label-info",
        "CANCELLED" : "label-warning",
        "FAILED" : "label-danger",
        "TIMED OUT" : "label-danger",
        "TERMINATED" : "label-danger",
        "COMPLETED" : "label-success"
    };

    $scope.statusMap = {
        "INCOMPLETE" : "label-info",
        "SCHEDULED" : "label-info",
        "STARTED" : "label-info",
        "FAILED" : "label-warning",
        "TIMED OUT" : "label-warning",
        "CANCELED" : "label-warning",
        "TERMINAL" : "label-danger",
        "COMPLETE" : "label-success",
        "CONTINGENT" : "label-default",
        "DEFERRED" : "label-info",
        "IMPOSSIBLE" : "label-danger"
    };

    $scope.timelineMap = {
        "ERROR" : "timeline-ERROR",
        "SUCCESS" : "timeline-SUCCESS",
        "WARNING" : "timeline-WARNING"
    };

    $scope.figureWorkflowStatusLabel = function(status) {
        return $scope.workflowStatusMap[status];
    };

    $scope.figureStatusLabel = function(status) {
        return $scope.statusMap[status];
    };

    $scope.figureTimelineStyle = function(eventType) {
        return $scope.timelineMap[eventType];
    };

    $scope.formatWhitespace = function(str) {
        return str.replace(/\n/g, '<br/>').replace(/\t/g, '&nbsp;&nbsp;&nbsp;');
    };

    $scope.div = function(contents, classes) {
        return "<div class='"+classes+"'>"+contents+"</div>";
    };

    $scope.span = function(contents, classes) {
        return "<span class='"+classes+"'>"+contents+"</span>";
    };

    $scope.jsonFormat = function(json, divclass) {
        if(json instanceof Array) {
            var body = "";
            for(var item in json) {
                body += $scope.div($scope.jsonFormat(json[item]));
            }
            return $scope.div(body, "block "+divclass);
        }
        if(json instanceof Object) {
            var body = "";
            for(var key in json) {
                body += $scope.div("<span><b>"+key+"</b></span> : "+$scope.jsonFormat(json[key], "block"));
            }
            return $scope.div(body, "block "+divclass);
        }
        if($scope.isString(json)) {
            return $scope.div($scope.formatURLs(json), divclass);
        }

        return json;
    };

    $scope.formatJson = function(jsonString) {
        return $scope.jsonFormat(JSON.parse(jsonString), "json");
    };

    $scope.formatJsonBasic = function(jsonString) {
        if(undefined == jsonString) { return jsonString; }
        return JSON.stringify(JSON.parse(jsonString), undefined, 4);
    };

    $scope.formatURLs = function(param) {
        var urlRegex = /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/ig;

        return param.replace(urlRegex, function (url) {

            if (( url.indexOf(".jpg") > 0 )
                || ( url.indexOf(".png") > 0 )
                || ( url.indexOf(".gif") > 0 )) {
                return '<img src="'+url+'">'+'<br/>'+url+'<br/>';
            }
            else {
                return '<a href="'+url+'">'+url+'</a>'+'<br/>';
            }
        });
    };

    $scope.isJSON = function(j) {
        return j[0] == '{';
    };

    $scope.isArray = function(a) {
        return a instanceof Array;
    };

    $scope.isString = function(s) {
        return typeof s === "string";
    };

    $scope.scrollToSection = function(name) {
        $location.hash("section_"+name);
        $anchorScroll();
    };

    $("#rawInputToggle").click(function() {
        $('#rawInput').slideToggle()
    });

});

app.controller('workersController', function($scope, $route, $http, $location, environment) {

    $scope.showDefunct = false;

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
        if(worker.minutesSinceLast < 2) {
            worker.freshness = "label-success";
        } else if(worker.minutesSinceLast < 5) {
            worker.freshness = "label-warning";
        } else {
            worker.freshness = "label-default";
            worker.defunct = true;
        }
    };

    $scope.categorizeWorkers = function() {
        $scope.domains = {};
        for(var w in $scope.workers) {
            var worker = $scope.workers[w];
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

app.controller('workflowInitiationController', function($scope, $route, $http) {

    $scope.loading = false;
    $scope.tags = [ { text: 'Dashboard:Initiated'}];

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
        $http.get('workflow/initiate', { params : params})
            .success(function(data) {
                         $scope.loading = false;
                         $scope.runId = data;
                     })
            .error(function(error) {
                       $scope.loading = false;
                       toastr.error(error.details, error.error)
                   });
    };

    $scope.addLabel = function() {
      $scope.tags.push({text : ""});
    };

});
