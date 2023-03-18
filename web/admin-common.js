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
            fullClass(val) {
                if (typeof val === 'undefined') return 'bg-disabled'
                if (isNaN(val)) return 'bg-disabled'
                if (val < 0.10) return 'bg-error'
                if (val < 0.25) return 'null'
                if (val < 0.50) return 'bg-recent'
                return 'bg-old'
            },
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
