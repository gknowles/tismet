/*
Copyright Glen Knowles 2022.
Distributed under the Boost Software License, Version 1.0.

about-common.js - tismet webapp
*/

//===========================================================================
function adminIntro(selected) {
    navTopIntro('Admin')
    addOpts({
        computed: {
            navSubSelected() {
                return selected
            },
        },
        methods: {
            navSub() {
                return [
                    { name: 'About', href: 'admin-about.html' },
                    { name: 'Backup' },
                    { name: 'Graphite' },
                ]
            },
        },
    })
    includeHtmlFragment('navbar-admin.html')
    document.currentScript.remove()
}
